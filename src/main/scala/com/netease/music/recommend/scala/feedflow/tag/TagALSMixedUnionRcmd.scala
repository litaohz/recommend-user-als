package com.netease.music.recommend.scala.feedflow.tag

import com.netease.music.recommend.scala.feedflow.tag.ALSTest.sim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSMixedUnionRcmd {

  def getItemidTypeM(itemData: Array[Row]) = {
    val itemIdtypeM = mutable.HashMap[Int, String]()
    for (row <- itemData) {
      val itemid = row.getAs[Int]("id")
      val idtype = row.getAs[String]("idtype")
      itemIdtypeM.put(itemid, idtype)
    }
    itemIdtypeM
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

    val model = MatrixFactorizationModel.load(sc, cmd.getOptionValue("als_model"))
    // val modelBroad = sc.broadcast(model)

    val itemData = spark.read.parquet(cmd.getOptionValue("item_factors")).select("id", "idtype").collect()
    val itemIdtypeM = getItemidTypeM(itemData)
    val itemIdtypeMBroad = sc.broadcast(itemIdtypeM)


    val sampleUserData = spark.read.parquet(cmd.getOptionValue("user_factors"))
      .filter($"userIdStr".isin("359792224", "303658303", "135571358", "2915298", "85493186", "41660744")).collect()
    val result = mutable.ArrayBuffer[String]()
    for (row <- sampleUserData) {
      val id = row.getAs[Int]("id")
      val userIdStr = row.getAs[String]("userIdStr")
      val uFeats = row.getAs[mutable.WrappedArray[Double]]("features")
      val results = model.recommendProducts(id, 100)
      val predicts = mutable.ArrayBuffer[String]()
      for (rs <- results) {
        val idtype = itemIdtypeMBroad.value.get(rs.product)
        predicts.append(idtype + ":" + rs.rating.toString)
      }
      val rcmddata = userIdStr + "\t" + predicts.mkString(",")
      result.append(rcmddata)
      println(rcmddata)
    }

    result.toDF("data").write.text(cmd.getOptionValue("output"))

    // model.recommendProducts(1, 10)
    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}