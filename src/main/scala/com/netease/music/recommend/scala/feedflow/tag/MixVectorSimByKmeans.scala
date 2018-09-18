package com.netease.music.recommend.scala.feedflow.tag

import java.io.BufferedReader
import java.util.Date

import breeze.linalg.DenseVector
import com.netease.music.recommend.scala.feedflow.GetVideoPool.{getCategoryId, getIds, getJsonValue}
import com.netease.music.recommend.scala.feedflow.tag.MixVectorKmeans.denVec
import com.netease.music.recommend.scala.feedflow.tag.MixedSimVideoUnionML.simScoreDotConsine
import com.netease.music.recommend.scala.feedflow.tag.RcmdByMatrixSim.getValidVideosList
import com.netease.music.recommend.scala.feedflow.tag.SimItemBySimtag.getVideoPredM
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{ColumnName, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * user vector * item vector
  * Created by hzlvqiang on 2018/1/22.
  */
object MixVectorSimByKmeans {



  def denVec = udf((features:mutable.WrappedArray[Float]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    for (f <- features) {
      doubleArray.append(f.toDouble)
    }
    Vectors.dense(doubleArray.toArray[Double])
  })

  def add(sumVec: ArrayBuffer[Double], features: org.apache.spark.ml.linalg.Vector) = {
    for (i <- 0 until features.size) {
      if (sumVec.size <= i) {
        sumVec.append(features(i))
      } else {
        sumVec(i) += features(i)
      }
    }
  }

  def mean(sumVec: ArrayBuffer[Double], size: Int) = {
    val meanV = mutable.ArrayBuffer[Double]()
    for (i <- 0 until sumVec.size) {
      meanV.append(sumVec(i) / size)
    }
    meanV
  }

  def getPredictionM(rows: Array[Row]) = {
    val predictionInfoM = mutable.HashMap[Int, (DenseVector[Double], mutable.WrappedArray[String])]()
    for (row <- rows) {
      val prediction = row.getAs[Int]("prediction")
      val meanFeatures = DenseVector(row.getAs[mutable.WrappedArray[Double]]("meanFeatures").toArray[Double])
      val idtypes = row.getAs[mutable.WrappedArray[String]]("idtypes")
      predictionInfoM.put(prediction, (meanFeatures, idtypes))
    }
    predictionInfoM
  }

  def simList(uFeatures: DenseVector[Double], predictionM: mutable.HashMap[Int, (DenseVector[Double], mutable.WrappedArray[String])]) = {
    val scoreNameListDot = mutable.ArrayBuffer[Tuple2[Double, mutable.WrappedArray[String]]]()
    var date = new Date()
    println("dot start:" + date.getTime)
    for ((prediction, (meanFeature, idtypes)) <- predictionM) {
      val scoreDot = uFeatures.dot(meanFeature)
      scoreNameListDot.append(Tuple2(scoreDot, idtypes))
    }
    val sortedNameListDot = scoreNameListDot.sortBy[Double](-_._1)
    sortedNameListDot
  }

  def getItemFeatM(rows: Array[Row]) = {
    val itemFeatM = mutable.HashMap[String, DenseVector[Double]]()
    for (row <- rows) {
      val idtype = row.getAs[String]("idtype")
      val dvec = row.getAs[org.apache.spark.ml.linalg.Vector]("features")
      val features = DenseVector(dvec.toArray)
      itemFeatM.put(idtype, features)
    }
    itemFeatM
  }


  def meanVec = udf((list: Seq[org.apache.spark.ml.linalg.Vector]) => {
    val sumVec = mutable.ArrayBuffer[Double]()
    for (features <- list) {
      add(sumVec, features)
    }
    val meanV = mean(sumVec, list.size)
    meanV
  })

  def getVideoTagsM(strings: Array[String]) = {
    val validVideos = new mutable.HashSet[String]()
    for(str <- strings) {
      implicit val formats = DefaultFormats
      val json = parse(str)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
    validVideos
  }

  def addWithPrefix(str: String, set: mutable.HashSet[String], pref: String) = {
    if (str.equals("0")) {
      null
    } else {
      for (s <- str.split("_tab_")) {
        set.add(pref + s)
      }
    }
  }


  def getVideoTagsM(rows: Array[Row]) = {
    val videoTagsM = mutable.HashMap[String, mutable.HashSet[String]]()
    for (row <- rows) {
      val idtype = row.getAs[Long]("videoId").toString + "-video"
      val tags = mutable.HashSet[String]()
      videoTagsM.put(idtype, tags)

      val category = row.getAs[String]("category")
      if (!category.equals("-1_-1")) {
        tags.add("C_" + category)
      }

      addWithPrefix(row.getAs[String]("artistIds"), tags, "A_")
      addWithPrefix(row.getAs[String]("videoBgmIds"), tags, "B_")
      addWithPrefix(row.getAs[String]("bgmIds"), tags, "B_")
      addWithPrefix(row.getAs[String]("userTagIds"), tags, "T_")
      addWithPrefix(row.getAs[String]("auditTagIds"), tags, "T_")
    }
    videoTagsM
  }

  def getVideoPredM(rows: Array[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      if (row.getAs[String]("vType").equals("video")) {
        val vid = row.getAs[Long]("videoId").toString
        val rawPred = row.getAs[Double]("rawPrediction")
        idtypePredM.put(vid + "-video", rawPred)
      }
    }
    idtypePredM
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("item_prediction", true, "item_prediction")
    options.addOption("user_features", true, "user_features")
    options.addOption("output", true, "output directory")

    options.addOption("Music_VideoRcmdMeta", true, "input")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("user_rced_video_from_event", true, "user_rced_video_from_event")
    options.addOption("user_video_pos", true, "user_video_pos")
    options.addOption("video_features", true, "video_features")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    println("视频审核表....")
    val videoRcmdMetaTable = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
      .withColumn("vid", $"videoId")
      .withColumn("category", getCategoryId($"category"))
      .withColumn("artistIds", getIds($"artistIds"))
      .withColumn("videoBgmIds", getIds($"videoBgmIds"))
      .withColumn("bgmIds", getIds($"bgmIds"))
      .withColumn("userTagIds", getIds($"userTagIds"))
      .withColumn("auditTagIds", getIds($"auditTagIds"))
      .withColumn("smallFlow", getJsonValue("smallFlow")($"extData"))
      .na.fill("null", Seq("title", "description"))
      .filter(!$"smallFlow")
    val videoTagsM = getVideoTagsM(videoRcmdMetaTable.collect())
    println("videoTagsM size:" + videoTagsM.size)
    val videoTagsMBroad = sc.broadcast(videoTagsM)



    println("读取用户已经推荐数据1...")
    val inputUserRced = cmd.getOptionValue("user_rced_video")
    val userRcedVideosData = spark.read.textFile(inputUserRced).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideos")
    println("读取用户已经推荐数据2...")
    val inputUserRcedFromEvent = cmd.getOptionValue("user_rced_video_from_event")
    val userRcedVideosFromEventData = spark.read.textFile(inputUserRcedFromEvent).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideosFromEvent")

    println("读取训练数据")
    val inputFromPref = spark.read.textFile(cmd.getOptionValue("user_video_pos"))
      .map(line => {
        val ts = line.split("\t")
        val id = ts(0)
        val uid = ts(1)
        (uid, id)
      }).toDF("userIdStr", "vid").groupBy("userIdStr")
      .agg(
        collect_list($"vid").as("rcedVideosFromPos"))
      .select("userIdStr", "rcedVideosFromPos")

    val userRecedVideosAll = userRcedVideosData
      .join(userRcedVideosFromEventData, Seq("userIdStr"), "outer")
      .join(inputFromPref,  Seq("userIdStr"), "outer")
      .map(row => {
        val userId = row.getAs[String]("userIdStr")
        val rcedSet = mutable.HashSet[String]()
        val rcedVideos = row.getAs[String]("rcedVideos")
        val rcedVideosFromEvent = row.getAs[String]("rcedVideosFromEvent")
        val rcedVideosFromPos = row.getAs[mutable.WrappedArray[String]]("rcedVideosFromPos")
        if (rcedVideos != null) {
          for (v <- rcedVideos.split(",")) {
            rcedSet.add(v)
          }
        }
        if (rcedVideosFromEvent != null) {
          for (v <- rcedVideosFromEvent.split(",")) {
            rcedSet.add(v)
          }
        }
        if (rcedVideosFromPos != null) {
          for (v <- rcedVideosFromPos) {
            rcedSet.add(v)
          }
        }
        (userId, rcedSet.mkString(","))
      }).toDF("userIdStr", "rcedVideos")
    userRecedVideosAll.show(2, false)

    //////////////////
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

    // 视频排序分
    val videoFeature = cmd.getOptionValue("video_features")
    var videoPredM = mutable.HashMap[String, Double]()
    if (videoFeature != null) {
      val viter = spark.read.parquet(videoFeature).toDF().select("videoId", "vType", "rawPrediction").collect()
      // println("vparray size:" + vparray.)
      videoPredM = getVideoPredM(viter)
    }
    //val videoPredM = getVideoPredM(viter)
    println("1742243 pred:" + videoPredM.get("1742243"))
    println("vieoPrefM size:" + videoPredM.size)
    val videoPredMBroad = sc.broadcast(videoPredM)

    val itemFeatM = getItemFeatM(itemPredictionInput.select("idtype", "features").collect())
    val itemFeatMBroad = sc.broadcast(itemFeatM)
    println("itemFeatMBroad size:" + itemFeatMBroad.value.size)

    val userFeatures = spark.read.parquet(cmd.getOptionValue("user_features"))
      //.filter($"userIdStr".isin("359792224", "303658303", "135571358", "2915298", "85493186", "41660744"))
      .withColumn("featuresVec", denVec($"features"))
      .select("id", "userIdStr", "featuresVec")

    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("features")
      .setP(1.0)

    val userFeaturesNorm =  normalizer.transform(userFeatures)
        .join(userRecedVideosAll, Seq("userIdStr"), "left")
      .map(row => {
        val uidstr = row.getAs[String]("userIdStr")
        // 去掉已推荐
        val rcedVideos = mutable.HashSet[String]()
        val rcedVideoStr = row.getAs[String]("rcedVideos")
        if (rcedVideoStr != null) {
          for (vtype <- rcedVideoStr.split(",")) {
            rcedVideos.add(vtype + "-video")
          }
        }

        val uFeatures = DenseVector(row.getAs[org.apache.spark.ml.linalg.Vector]("features").toArray)
        // 求最近聚类
        val sortedPredictionList = simList(uFeatures, predictionMBroad.value) // .ArrayBuffer[Tuple2[Double, mutable.WrappedArray[String]]]

        // 求聚类中的近似idtype
        val candIdtypleList = mutable.ArrayBuffer[Tuple2[Double, String]]()
        var i = 0
        // 10000 / 1000 = 最低10个分类
        while (candIdtypleList.size < 10000 && i < sortedPredictionList.size) {
          val predIter = sortedPredictionList(i)._2.iterator
          val curNumm = candIdtypleList.size
          while (predIter.hasNext && candIdtypleList.size < curNumm + 1000) {
            val idtype = predIter.next()
            if (!rcedVideos.contains(idtype)) {
              val itemFeatVec = itemFeatMBroad.value.getOrElse(idtype, null)
              var score = 0.0
              if (itemFeatVec != null) {
                score = uFeatures.dot(itemFeatVec) * (1.0 + videoPredMBroad.value.getOrElse(idtype, 0.0))
                candIdtypleList.append((score, idtype))
              }
            }
          }
          i += 1
        }

        val sortedCands = candIdtypleList.sortBy[Double](-_._1)
        val result = mutable.ArrayBuffer[String]()
        // 限定tag数量
        val tagCntM = mutable.HashMap[String, Int]()
        val iter = sortedCands.iterator
        while (iter.hasNext && result.size < 50) {
          val cand = iter.next()
          val idtype = cand._2
          if (!rcedVideos.contains(idtype)) {
            val tags = videoTagsMBroad.value.get(idtype)
            var valid = true
            if (tags != null && tags != None) {
              for (tag <- tags.get) {
                val cnt = tagCntM.getOrElse(tag, 0)
                if (cnt >= 5) {
                  valid = false
                }
                tagCntM.put(tag, cnt + 1)
              }
            }
            if (valid) {
              //result.append(idtype.replace("-", ":") + ":" + cand._1.toString.substring(0, 8))
              result.append(idtype.replace("-", ":"))
            }
          }
        }

        uidstr + "\t" + result.mkString(",")
      }).write.text(cmd.getOptionValue("output"))
 /*


    val validVideos = getValidVideosList(spark.read.textFile(cmd.getOptionValue("Music_VideoRcmdMeta")).collect())
    println("validVideos size:" + validVideos.size)
    val validVideosBroad = sc.broadcast(validVideos)
    print ("itemFactorsIdtypeM size:" + validVideosBroad.value.size)


    import spark.implicits._
    // itemFeature
    val itemFeatures = spark.read.parquet(cmd.getOptionValue("item_features")) // id, idtype, features
      .filter($"idtype".isin(validVideosBroad.value.toArray[String]:_*))
      .withColumn("featuresVe", denVec($"features"))
      .select("id", "idtype", "featuresVec")
    // 归一化
    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("features")
      .setP(1.0)

    val itemFeaturesNorm = normalizer.transform(itemFeatures)
    val kmeans = new KMeans().setK(800).setSeed(800) // 800 * 800 = 当前有效视频数
    val kMeansModel = kmeans.fit(itemFeaturesNorm)
    val predResult = kMeansModel.transform(itemFeaturesNorm) // prediction
    println("predResult schema...")
    predResult.printSchema() //
    predResult.show(10, false)
    predResult.write.parquet(cmd.getOptionValue("output"))
*/
  }

}
