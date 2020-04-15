package cn.diana.algorithm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSDemo {

  val redisUtils = new RedisUtils //单机版的redis方法
  redisUtils.init("ip", 6379, 1000, "password")

  def main(args: Array[String]): Unit = {

    //1 构建Spark对象
    val sc = createSc()

    //2 读取样本数据
    val ratings = fetchData(sc)

    //3 建立模型
    val model = createModel(ratings)

    //4 预测结果
    predict(ratings, model)

    //5 保存/加载模型
    modelSaveAndfetch(model, sc)

    //6 关闭sc
    sc.stop()

  }

  def createSc(): SparkContext ={
    val spark = SparkSession.builder().appName("ALSModel").master("local").getOrCreate()
    spark.sparkContext
  }

  def fetchData(sc: SparkContext): RDD[Rating] ={
    val data = sc.textFile("./data/test.data")
    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })
    ratings
  }

  def createModel(ratings: RDD[Rating]): MatrixFactorizationModel ={
    val rank = 3
    val numIterations = 5
    ALS.train(ratings, rank, numIterations, 0.01)
  }

  def predict(ratings: RDD[Rating], model: MatrixFactorizationModel): Unit ={
    val usersProducts = ratings.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    val predictions =
      model.predict(usersProducts).map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }
    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
    println("直接调用模型中的推荐：" + model.recommendProducts(1, 10).toList)
  }

  /**
   * 模型存储redis的方式
   * ALS训练后模型为MatrixFactorizationModel
   * 查看源码可看到MatrixFactorizationModel有三个参数：rank: Int，val userFeatures: RDD[(Int, Array[Double])], productFeatures: RDD[(Int, Array[Double])])
   * @param model
   */
  def modelSaveRedis(model: MatrixFactorizationModel): Unit ={
    val rankSave = model.rank
    val userFeaturesSave = model.userFeatures.map(k => (k._1, k._2.toList)).collect().toList
    val productFeaturesSave = model.productFeatures.map(k => (k._1, k._2.toList)).collect().toList
    val json = new JSONObject()
    json.put("rank", rankSave.toString)
    json.put("userFeatures", toJson(userFeaturesSave))
    json.put("productFeatures", toJson(productFeaturesSave))

    redisUtils.saveModel("alsmodel", json.toJSONString)
  }

  /**
   * 获取redis中的模型
   * @param sc
   * @return
   */
  def fetchRedisModel(sc: SparkContext): MatrixFactorizationModel ={
    val sameModel = redisUtils.getModel("alsmodel")

    val modelJson = JSON.parseObject(sameModel)
    val sameRank = modelJson.getIntValue("rank")
    val sameUserFea = turnFeature(modelJson.getString("userFeatures"))
    val sameProductFea = turnFeature(modelJson.getString("productFeatures"))

    val sameUserFeaRdd = sc.parallelize(sameUserFea).map( k => (k._1, k._2.toArray))
    val sameProductFeaRdd = sc.parallelize(sameProductFea).map( k => (k._1, k._2.toArray))

    new MatrixFactorizationModel(sameRank, sameUserFeaRdd, sameProductFeaRdd)
  }

  def modelSaveAndfetch(model: MatrixFactorizationModel, sc: SparkContext): Unit ={
    // 常用方式
    // model.save(sc, "./myModelPath") // 持久化
    // val alsModel = MatrixFactorizationModel.load(sc, "./myModelPath") // 获取模型

    // redis 方式
    modelSaveRedis(model)  // 持久化
    val alsModel = fetchRedisModel(sc) // 获取模型

    println("获取到模型后的推荐：" + alsModel.recommendProducts(1, 10).toList)
  }

  def toJson(value: Any): String = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(value)
  }

  def turnFeature(str: String): List[(Int, List[Double])] ={
    var list = List[(Int, List[Double])]()
    val jsonList = JSON.parseArray(str).toArray().toList

    list = jsonList.map(k => {
      val element = JSON.parseArray(k.toString)
      val one = element.getIntValue(0)
      val two = JSON.parseArray(element.getString(1)).toArray().toList.map(_.toString.toDouble)
      (one, two)
    })
    list
  }

}