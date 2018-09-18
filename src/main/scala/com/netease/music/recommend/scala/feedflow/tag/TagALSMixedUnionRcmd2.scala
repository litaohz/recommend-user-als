package com.netease.music.recommend.scala.feedflow.tag

import com.netease.music.recommend.scala.feedflow.tag.MixedSimVideo.sim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSMixedUnionRcmd2 {

  def getItemidTypeM(itemData: Array[Row]) = {
    val itemIdtypeM = mutable.HashMap[String, mutable.WrappedArray[Double]]()
    for (row <- itemData) {
      // val itemid = row.getAs[Int]("id")
      val idtype = row.getAs[String]("idtype")
      val feats = row.getAs[mutable.WrappedArray[Double]]("features")
      itemIdtypeM.put(idtype, feats)
    }
    itemIdtypeM
  }

  def sim(itemFactorsM: mutable.HashMap[String, mutable.WrappedArray[Double]], factors: mutable.WrappedArray[Double]) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Double, String]]()
    for ((idtype, candFacotrs) <- itemFactorsM) {
      val score = simScore(candFacotrs, factors)
      scoreNameList.append(Tuple2(score, idtype))
    }
    val sortedNameList = scoreNameList.sortBy[Double](-_._1)
    sortedNameList
  }

  def simScore(candFactors: mutable.WrappedArray[Double], factors: mutable.WrappedArray[Double]) = {
    var product =  0.0
    var sqrt1 = 0.0
    var sqrt2 = 0.0
    for (i <- 0 until candFactors.length) {
      product += candFactors(i) * factors(i)
      sqrt1 += candFactors(i) * candFactors(i)
      sqrt2 += factors(i) * factors(i)
    }
    //TODO
    product
    // product / (math.sqrt(sqrt1.toDouble) * math.sqrt(sqrt2.toDouble)).toDouble
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("user_factors", true, "user_factors")
    options.addOption("item_factors", true, "item_factors")
    options.addOption("als_model", true, "als_model")
    options.addOption("output", true, "output")
    // options.addOption("modelOutput", true, "output directory")
    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    //val model = MatrixFactorizationModel.load(sc, cmd.getOptionValue("als_model"))
    // val modelBroad = sc.broadcast(model)

    val idtypeFeats = getItemidTypeM(spark.read.parquet(cmd.getOptionValue("item_factors")).filter($"idtype".endsWith("-video")).select("idtype", "features").collect())
    val idtypeFeatsMBroad = sc.broadcast(idtypeFeats)


    val sampleUserData = spark.read.parquet(cmd.getOptionValue("user_factors"))
      .filter($"userIdStr".isin("359792224", "303658303", "135571358", "2915298", "85493186", "41660744")).collect()
    val result = mutable.ArrayBuffer[String]()
    for (row <- sampleUserData) {
      val id = row.getAs[Int]("id")
      val userIdStr = row.getAs[String]("userIdStr")
      val uFeats = row.getAs[mutable.WrappedArray[Double]]("features")
      val simNameScoreList = sim(idtypeFeatsMBroad.value, uFeats)
      val topSimNameScoreList = mutable.ArrayBuffer[String]()
      for (i <- 0 until 100) {
        topSimNameScoreList.append(simNameScoreList(i)._2 + ":" + simNameScoreList(i)._1)
      }
      val rcmddata = userIdStr + "\t" + topSimNameScoreList.mkString(",")
      result.append(rcmddata)
      println(rcmddata)
    }

    result.toDF("data").write.text(cmd.getOptionValue("output"))

    // model.recommendProducts(1, 10)
    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}