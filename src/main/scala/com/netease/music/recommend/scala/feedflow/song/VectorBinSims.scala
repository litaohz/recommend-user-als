package com.netease.music.recommend.scala.feedflow.song

import java.util.Date

import breeze.linalg.DenseVector
import com.netease.music.recommend.scala.feedflow.tag.MixVectorSimByKmeans.{getItemFeatM, getPredictionM, meanVec}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by hzlvqiang on 2018/4/27.
  */
object VectorBinSims {

  def simItems(predictionM: mutable.HashMap[Int, (DenseVector[Double], mutable.WrappedArray[String])], itemFeatsM: mutable.HashMap[String, DenseVector[Double]])
              = udf((itemFeatLV: org.apache.spark.ml.linalg.Vector) => {
    val itemFeatures = DenseVector(itemFeatLV.toArray)

    val scoreNameListDot = mutable.ArrayBuffer[Tuple2[Double, mutable.WrappedArray[String]]]()
    for ((prediction, (meanFeature, idtypes)) <- predictionM) {
      val scoreDot = itemFeatures.dot(meanFeature)
      scoreNameListDot.append(Tuple2(scoreDot, idtypes))
    }
    val sortedNameListDot = scoreNameListDot.sortBy[Double](-_._1)

    val iter = sortedNameListDot.iterator
    val simItemsScoreList = mutable.ArrayBuffer[(String, Double)]()
    for ((score, idtypes) <- sortedNameListDot if simItemsScoreList.size < 1000) {
      for (idtype <- idtypes) {
        val score = itemFeatures.dot(itemFeatsM.get(idtype).get)
        simItemsScoreList.append((idtype, score))
      }
    }
    val sortedSimItemsScoreList = simItemsScoreList.sortBy[Double](-_._2)
    val result = mutable.ArrayBuffer[String]()
    for ((idtype, score) <- sortedSimItemsScoreList if result.size < 100) {
      result.append(idtype + ":" + score)
    }
    result.mkString(",")
  })

  // 分桶
  def splitKeys(feats: Seq[Double], modbase: Int, idtype: String) = {
    val resKeys = mutable.ArrayBuffer[(String, String)]()
    val featsStr = feats.mkString(",")
    var allBits = new mutable.StringBuilder()
    for (i <- 0 until feats.size) {
      if (feats(i) > 0) {
        allBits.append("1")
      } else {
        allBits.append("0")
      }
    }
    val value = idtype + "\t" + featsStr
    var curKeys = new mutable.StringBuilder()
    curKeys.append("0_")
    for (i <- 0 until feats.size) {
      if (feats(i) > 0) {
        curKeys.append("1")
      } else {
        curKeys.append("0")
      }
      if ((i + 1) % modbase == 0) {
        resKeys.append((curKeys.toString, allBits.toString() + "\t" + value))
        curKeys = new mutable.StringBuilder()
        curKeys.append(i).append("_")
      }
    }
    resKeys
  }

  def string2vector(strings: Array[String]) = {
    val resDs = mutable.ArrayBuffer[Double]()
    for (str <- strings) {
      resDs.append(str.toDouble)
    }
    DenseVector(resDs.toArray)
  }

  def getTopSims(maxNum: Int) = udf((simscoresList: Seq[String]) => {
    val simScoreM = mutable.HashMap[String, Double]()
    for (simscores <- simscoresList) {
      for (simscore <- simscores.split(",")) {
        val ss = simscore.split(":")
        simScoreM.put(ss(0), ss(1).toDouble)
      }
    }
    val sortedSimscore = simScoreM.toArray.sortWith(_._2 > _._2)
    val res = mutable.ArrayBuffer[String]()
    for ((sim, score) <- sortedSimscore if res.size < maxNum) {
      res.append(sim + ":" + score)
    }
    res.mkString(",")
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("item_features", true, "item_prediction")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    val itemFeatures = spark.read.textFile(cmd.getOptionValue("item_features")) // id float1,float2
      .map(line => {
      val ts = line.split("\t")
      val item = ts(0)
      val vecs = mutable.ArrayBuffer[Double]()
      for (v <- ts(1).split(",")) {
        vecs.append(v.toDouble)
      }
      (item, vecs)
    }).toDF("idtype", "features")

    println("itemFeatures size:" + itemFeatures.count())
    itemFeatures.show(2, false)

    val binIdtypeFeats = itemFeatures
      .flatMap(row => {
        // val result = mutable.ArrayBuffer[String]()
        val idtype = row.getAs[String]("idtype")
        val vectorFeat = row.getAs[Seq[Double]]("features")
        val binkeys = splitKeys(vectorFeat.toArray, 8, idtype) // 分成16个桶
        binkeys
      }).toDF("binKey", "allkey_idtype_features")

    println("binIdtypeFeats szie:" + binIdtypeFeats.count())
    binIdtypeFeats.show(2, false)

    val itemSimsData1 = binIdtypeFeats
      .groupBy("binKey").agg(collect_list("allkey_idtype_features").as("allkey_idtype_features_list"))
      .flatMap(row => {
        val res = mutable.ArrayBuffer[(String, Seq[String]) ]()
        val binKey = row.getAs[String]("binKey")
        val keyIdtypeFeatList = row.getAs[Seq[String]]("allkey_idtype_features_list")
        val maxNumCnt = keyIdtypeFeatList.size / 1000 + 1
        if (maxNumCnt > 2) {
          val newBinKeyM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
          val bitNum = math.log(maxNumCnt).toInt + 1
          var binKeyStart = binKey.split("_")(0).toInt
          binKeyStart += 8
          for (keyIdtypeFeat <- keyIdtypeFeatList) {
            var newBinKey = binKey
            val kif = keyIdtypeFeat.split("\t", 2)
            val allKey = kif(0)
            if (binKeyStart + bitNum >= allKey.size) {
              println("allKey makeup")
              println(allKey)
              println(binKeyStart)
              println(bitNum)
              newBinKey += "_" // 区别前后加01串
              binKeyStart = 0
            }
            for (i <- binKeyStart until (binKeyStart + bitNum)) {
              newBinKey += allKey.charAt(i)
            }
            val newKIF = newBinKeyM.getOrElse(newBinKey, mutable.ArrayBuffer[String]())

            newKIF.append(keyIdtypeFeat)
            newBinKeyM.put(newBinKey, newKIF)
          }
          res.appendAll(newBinKeyM)
        } else {
            res.append((binKey, keyIdtypeFeatList))
        }
        res
      }).toDF("binKey", "allkey_idtype_features")
    println("itemSimsData1 sample...")
    itemSimsData1.show(10, false)

    val itemSimsData = itemSimsData1.flatMap(row => {
        val curIdtypeSims = mutable.ArrayBuffer[String]()
        val idtypeFeatList = row.getAs[Seq[String]]("allkey_idtype_features")
        val idtypeVecList = mutable.ArrayBuffer[(String, DenseVector[Double])]()
        for (idtypeFeats <- idtypeFeatList) {
          val idtFt = idtypeFeats.split("\t")
          val idtype = idtFt(1)
          val feats = string2vector(idtFt(2).split(","))
          idtypeVecList.append((idtype, feats))
        }
        // 同一个桶里面，计算相似
        val idtypeSimscoresM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Double)]]()
        for (i <- 0 until idtypeVecList.size) {
          for (j <- i+1 until idtypeVecList.size) {
            val score = idtypeVecList(i)._2.dot(idtypeVecList(j)._2)
            val simscoresI = idtypeSimscoresM.getOrElse(idtypeVecList(i)._1, mutable.ArrayBuffer[(String, Double)]())
            simscoresI.append((idtypeVecList(j)._1, score))
            idtypeSimscoresM.put(idtypeVecList(i)._1, simscoresI)

            val simscoresJ = idtypeSimscoresM.getOrElse(idtypeVecList(j)._1, mutable.ArrayBuffer[(String, Double)]())
            simscoresJ.append((idtypeVecList(i)._1, score))
            idtypeSimscoresM.put(idtypeVecList(j)._1, simscoresJ)
          }
        }

        // 相似候选排序
        val sortedIdtypeSims = mutable.ArrayBuffer[(String, String)]()
        for ((idtype, simscores) <- idtypeSimscoresM) {
          val sortedSims = simscores.sortWith(_._2 > _._2)
          val sortedStrs = mutable.ArrayBuffer[String]()
          for (sim <- sortedSims if sortedStrs.size < 40) {
            sortedStrs.append(sim._1 + ":" + sim._2)
          }
          sortedIdtypeSims.append((idtype, sortedStrs.mkString(",")))
        }
        sortedIdtypeSims
      }).toDF("idtype", "simscores")
      .groupBy("idtype").agg(collect_list($"simscores").as("simscores_list"))
      .withColumn("topSimsocres", getTopSims(60)($"simscores_list"))
      .select("idtype", "topSimsocres")

    itemSimsData.write.parquet(cmd.getOptionValue("output"))

    /*
    val itemPredictionInput = spark.read.parquet(cmd.getOptionValue("item_prediction"))
    itemPredictionInput.printSchema()
    println("itemPredictionInput sample...")
    itemPredictionInput.show(5, false)
    val itemPredictionInfo = itemPredictionInput
      .groupBy("prediction")
      .agg(
        collect_list($"idtype").as("idtypes"),
        collect_list($"features").as("featuresList"))
      .withColumn("meanFeatures", meanVec($"featuresList"))
      .select("prediction", "meanFeatures", "idtypes")

    val predictionM = getPredictionM(itemPredictionInfo.collect())
    println("predictionM size:" + predictionM.size)
    val predictionMBroad = sc.broadcast(predictionM)

    val itemFeatM = getItemFeatM(itemPredictionInput.select("idtype", "features").collect())
    println("itemFeatM size:" + itemFeatM.size)
    val itemFeatMBroad = sc.broadcast(itemFeatM)
    println("itemFeatM value size:" + itemFeatMBroad.value.size)

    val idtypeSimsData = itemPredictionInput.repartition(2000).withColumn("simItems", simItems(predictionMBroad.value, itemFeatMBroad.value)($"features"))
      .select("idtype", "simItems")

    idtypeSimsData.write.parquet(cmd.getOptionValue("output") + "/sims")
    */
  }

}
