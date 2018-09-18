package com.netease.music.recommend.scala.feedflow.tag

import java.io.{BufferedReader, InputStreamReader}
import java.util.Date

import breeze.linalg.DenseVector
import com.netease.music.recommend.scala.feedflow.GetVideoPool.getJsonValue
import com.netease.music.recommend.scala.feedflow.hot.GetHotVideoFromSpecUpUser.isInSet
import com.netease.music.recommend.scala.feedflow.tag.MixedSimVideoTestDot.denVec
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

/**
  * word2vec
  */
object MixedSimVideoNormDotByPref {

  def vector2vector(features: linalg.DenseVector): _root_.breeze.linalg.DenseVector[Float] = {
    val floatFeatures = mutable.ArrayBuffer[Float]()
    for (f <- features.toArray) {
      floatFeatures.append(f.toFloat)
    }
    breeze.linalg.DenseVector[Float](floatFeatures.toArray[Float])
  }

  def getItemFactorM(rows: Array[Row]) = {
    println("row nums:" + rows.length)
    val itemFactorM = mutable.HashMap[String, DenseVector[Float]]()
    for (row <- rows) {
      val idtype = row.getAs[String]("idtype")
      val features = row.getAs[org.apache.spark.ml.linalg.DenseVector]("normFeatures")
      itemFactorM.put(idtype, vector2vector(features))
      /*
      val features = row.getAs[mutable.WrappedArray[Double]]("normFeatures")
      val floatFeatures = mutable.ArrayBuffer[Float]()
      for (feat <- features) {
        floatFeatures.append(feat.toFloat)
      }
      itemFactorM.put(idtype, DenseVector[Float](floatFeatures.toArray[Float]))
      */
    }
    itemFactorM
  }

  def simDot(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, DenseVector[Float]]], factors: DenseVector[Float]) = {
    val scoreNameListDot = mutable.ArrayBuffer[Tuple2[Float, String]]()

    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      val xy = candFacotrs.dot(factors)
      //val (scoreDot, scoreDotconsine) = simScoreDotConsine(candFacotrs, factors)
      scoreNameListDot.append(Tuple2(xy, idtype))
    }
    //scoreNameList.sortBy[Float](t2 => t2._1, false)
    val sortedNameListDot = scoreNameListDot.sortBy[Float](-_._1)

    sortedNameListDot
  }

  def sim(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, DenseVector[Float]]], factors: DenseVector[Float]) = {
    val scoreNameListDot = mutable.ArrayBuffer[Tuple2[Float, String]]()
    val scoreNameListDotCosine = mutable.ArrayBuffer[Tuple2[Float, String]]()
    var date = new Date()
    println("dot start:" + date.getTime)
    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      val (scoreDot, scoreDotconsine) = simScoreDotConsine(candFacotrs, factors)
      scoreNameListDot.append(Tuple2(scoreDot, idtype))
      scoreNameListDotCosine.append(Tuple2(scoreDotconsine, idtype))
    }
    date = new Date()
    println("dot end:" + date.getTime)
    //scoreNameList.sortBy[Float](t2 => t2._1, false)
    val sortedNameListDot = scoreNameListDot.sortBy[Float](-_._1)
    val sortedNameListDotCosine = scoreNameListDotCosine.sortBy[Float](-_._1)

    // for 循环排序
    val scoreNameListCosine = mutable.ArrayBuffer[Tuple2[Float, String]]()
    date = new Date()
    println("cosine start:" + date.getTime)
    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      val cosine = simScoreConsine(candFacotrs, factors)
      scoreNameListCosine.append(Tuple2(cosine, idtype))
    }
    date = new Date()
    println("cosine end:" + date.getTime)
    val sortedNameListCosine = scoreNameListCosine.sortBy[Float](-_._1)

    (sortedNameListDot, sortedNameListDotCosine, sortedNameListCosine)
  }

  def simScoreDotConsine(candFactors: DenseVector[Float], factors: DenseVector[Float]) = {

    val xy = candFactors.dot(factors)
    val xx = Math.sqrt(candFactors.dot(candFactors))
    val yy = Math.sqrt(factors.dot(factors))
    (xy.toFloat, (xy / (xx * yy)).toFloat)
  }

  def simScoreConsine(candFactors: DenseVector[Float], factors: DenseVector[Float]) = {

    var product =  0.0
    var sqrt1 = 0.0
    var sqrt2 = 0.0
    for (i <- 0 until candFactors.length) {
      product += candFactors(i) * factors(i)
      sqrt1 += candFactors(i) * candFactors(i)
      sqrt2 += factors(i) * factors(i)
    }
    // dot cosine
    (product / (math.sqrt(sqrt1.toFloat) * math.sqrt(sqrt2.toFloat)).toFloat).toFloat

  }

  def getValidVideosList(strings: Array[String]) = {
    val validVideos = new mutable.HashSet[String]()
    for(str <- strings) {
      implicit val formats = DefaultFormats
      val json = parse(str)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
    validVideos
  }

  def getValidVideos(inputDir: String) = {
    val validVideos = new mutable.HashSet[String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir))) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getValidVideosFromFile(bufferedReader, validVideos)
        }
      }
    }
    validVideos
  }

  def getValidVideosFromFile(reader: BufferedReader, validVideos: mutable.HashSet[String]): Unit = {
    var line = reader.readLine()
    while (line != null) {
      //try {
      implicit val formats = DefaultFormats
      val json = parse(line)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
  }

  def getValidIdtypes(rows: Array[Row]) = {
    val validvids = mutable.HashSet[String]()
    for (row <- rows) {
      validvids.add(row.getAs[Long]("videoId").toString + "-video")
    }
    validvids
  }

  def isValid(validVids: Broadcast[mutable.HashSet[String]]) = udf((idtype: String) => {
    var res = false
    if (validVids.value.contains(idtype)) {
      res = true
    }
    res
  })

  def double2Float(doubles: mutable.WrappedArray[Double]) = {
    val floats = mutable.ArrayBuffer[Float]()
    for (d <- doubles) {
      floats.append(d.toFloat)
    }
    floats.toArray[Float]
  }

  def getItemSimsM(strings: Array[Row]) = {
    val vidSimsM = mutable.HashMap[String, String]()
    for(row <- strings) {
      // item \t vid:vtype:score,vid:vtype:score
      val item = row.getAs[String]("idtype")
      val idwts = row.getAs[mutable.WrappedArray[String]]("vidwts")
      // item -> vid,vid
      vidSimsM.put(item, idwts.mkString(","))
    }
    vidSimsM
  }

  def isInSet(idtypes: mutable.HashSet[String]) = udf((vid: String) => {
    if (idtypes.contains(vid + "-video")) {
      true
    } else {
      false
    }
  })

  def getVidPredictionM(rows: Array[Row]) = {
    val vidPredM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      val vid = row.getAs[Long]("videoId").toString
      val pred = row.getAs[Double]("rawPrediction")
      vidPredM.put(vid, pred)
    }
    vidPredM
  }

  def toLong = udf((uidstr: String) => {
    uidstr.toLong
  })

  def isNotEmpty = udf((arrays: Seq[String]) => {
    if (arrays == null || arrays.isEmpty) {
      false
    } else {
      true
    }
  })

  def isValidIdtype = udf((idtype: String, userCnt:Long) =>{
    var valid = true
    if (idtype.endsWith("video")) {
      if (userCnt < 100) {
        valid = false
      }
    } else {
      if (userCnt < 200) {
        valid = false
      }
    }
    valid
  })

  def rcmd(itemFactorM: Broadcast[mutable.HashMap[String, breeze.linalg.DenseVector[Float]]],  vidPredictionMBroad: Broadcast[mutable.HashMap[String, Double]])
      = udf((userIdStr: String, unormFreature: org.apache.spark.ml.linalg.DenseVector, srcSvidwtsList:Seq[Seq[String]]) => {
    var userFactors = DenseVector[Float]()
    if (unormFreature != null) {
      userFactors = vector2vector(unormFreature)
    }
    if (userIdStr.equals("359792224")) {
      println("userFactor for 359792224")
      println(userFactors)
    }
    /*
    val videoWts = row.getAs[mutable.HashMap[String, Double]]("videoWtM")
    for (itemwt <- prefItemWts) {
      val vid = vidwt._1

    }*/
    // TODO rating
    val rcmdsALLM = mutable.HashMap[String, (Double, String)]()
    for (sims4SameSrc <- srcSvidwtsList) {
      if (!sims4SameSrc.isEmpty) {
        val rcmdsForSinglePref = mutable.ArrayBuffer[(String, Double, String)]()
        val src = sims4SameSrc(0)
        for (i <- 1 until sims4SameSrc.size) {
          val svidWt = sims4SameSrc(i)
          val svidwt = svidWt.split(":")
          val swt = svidwt(1).toDouble
          val videoFactors= itemFactorM.value.get(svidwt(0) + "-video")
          if (userIdStr.equals("359792224")) {
            println("video factor for:" + svidwt(0) + "-video")
            println(videoFactors)
          }
          var dotScore = 1.0
          if (videoFactors != null && videoFactors!= None && userFactors != None && userFactors != None && userFactors.length > 0) {
            dotScore = dotScore + userFactors.dot(videoFactors.get)
          }
          var prediction = 1.0 + vidPredictionMBroad.value.getOrElse(svidwt(0), 0.0)
          rcmdsForSinglePref.append((svidwt(0), dotScore * prediction * swt, src))
        }
        // val sortedrcmdsForSinglePref = rcmdsForSinglePref.sortWith(_._2 > _._2)
        for (i <- 0 until rcmdsForSinglePref.size) {

          val rcmdVideo = rcmdsForSinglePref(i)._1
          val rcmdwt = rcmdsForSinglePref(i)._2
          var srcReason = rcmdsForSinglePref(i)._3
          if (!srcReason.contains("-")) {
            srcReason = srcReason + "-song"
          }
          val wtSrc = rcmdsALLM.getOrElse(rcmdsForSinglePref(i)._1, null)
          if (wtSrc == null) {
            val rvideo =
            rcmdsALLM.put(rcmdVideo, (rcmdwt, srcReason))
          } else {
            rcmdsALLM.put(rcmdVideo, (wtSrc._1+rcmdwt, wtSrc._2+"&"+srcReason))
          }
        }
      }
    }
    val sortedRcmds = rcmdsALLM.toArray[(String, (Double, String))].sortWith(_._2._1 > _._2._1)
    val result = mutable.ArrayBuffer[String]()
    val reasonCntM = mutable.HashMap[String, Int]()
    for (i <- 0 until sortedRcmds.size if result.length < 40) {
      var reasons = sortedRcmds(i)._2._2
      var maxReasonCnt = 0
      for (reason <- reasons.split("&")) {
        val reasonCnt = reasonCntM.getOrElse(reason, 0)
        reasonCntM.put(reason,reasonCnt + 1)
        if (reasonCnt > maxReasonCnt) {
          maxReasonCnt = reasonCnt
        }
      }
      if (maxReasonCnt < 5) {
        result.append(sortedRcmds(i)._1 + ":video:" + reasons)
      }

    }
    if (result.size > 0) {
      userIdStr + "\t" + result.mkString(",")
    } else {
      null
    }
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("item_feature", true, "input")
    options.addOption("user_feature", true, "input")
    options.addOption("user_item_rating", true, "input")
    options.addOption("user_pref", true, "input")
    options.addOption("item_sims", true, "input")
    options.addOption("video_features", true, "input")
    options.addOption("Music_VideoRcmdMeta", true, "input")
    options.addOption("item_cnt", true, "input")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputItemPath = cmd.getOptionValue("item_feature")
    val outputPath = cmd.getOptionValue("output")
    println("加载视频meta，相似视频限定在这些视频内")
    val videoRcmdMetaTable = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
      .withColumn("videoId", $"videoId")
      .withColumn("smallFlow", getJsonValue("smallFlow")($"extData"))
      .filter(!$"smallFlow")
    val validVidtypes = getValidIdtypes(videoRcmdMetaTable.collect())
    val validVidtypesBroad = sc.broadcast(validVidtypes)
    println("validVidtypes: " + validVidtypes.head)
    println("validVidtypes size:" + validVidtypes.size)
    println("#item factor...")
    val itemFactors = spark.read.parquet(inputItemPath)
    println("itemFactors schema:")
    itemFactors.printSchema()
    itemFactors.show(10, false)

    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("normFeatures")
      .setP(1.0)
    println("对itemFactor向量进行归一处理")
    val itemFactorsNorm = normalizer.transform(itemFactors.withColumn("featuresVec", denVec($"features")))
    println("Norm schema...")
    itemFactorsNorm.printSchema()
    itemFactorsNorm.show(2, false)

    println("相似视频限定在有效视频内")
    println("itemFactorsNorm head:" + itemFactorsNorm.filter($"idtype".endsWith("video")).head)
    println("validVidtypesBroad head: " + validVidtypesBroad.value.head)
    val t1 = itemFactorsNorm.withColumn("isValid", isValid(validVidtypesBroad)($"idtype"))
      .filter($"isValid")
    println(t1.count())
    println("t1 head:" + t1.head())
    t1.printSchema()
    println(t1.select("normFeatures").head(2))
    val validItemFactorsNorm = t1
      .filter($"normFeatures".isNotNull)
    println("validItemFactorsNorm")
    validItemFactorsNorm.show(2, false)
    val validItemFactorsNormM = getItemFactorM(validItemFactorsNorm.collect())
    print ("itemFactorsIdtypeM size:" + validItemFactorsNormM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[String, breeze.linalg.DenseVector[Float]]](validItemFactorsNormM)
    println(validItemFactorsNormM.head)

    println("#item sims")
    val itemCntData = spark.read.parquet(cmd.getOptionValue("item_cnt"))
    val itemSimsData = spark.read.textFile(cmd.getOptionValue("item_sims")).map(row => {
      // item \t vid-vtype:score,vid-vtype:score
      val ivs = row.split("\t", 2)
      val idwts = mutable.ArrayBuffer[String]()
      idwts.append(ivs(0))
      for (vs <- ivs(1).split(",")) {
        val idtypewt = vs.split(":")
        if (idtypewt(0).endsWith("video")) {
          idwts.append(idtypewt(0).split("-")(0) + ":" + idtypewt(1))
        } else {
          idwts.append(idtypewt(0) + ":" + idtypewt(1))
        }
      }
      // item -> vid,vid
      (ivs(0), idwts)
    }).toDF("idtype", "srcSvidwts")
    itemSimsData.show(2, false)

    println("#item prediction")
    val vidPredictionM = getVidPredictionM(spark.read.parquet(cmd.getOptionValue("video_features"))
      .withColumn("isValid", isInSet(validVidtypes)($"videoId"))
      .filter($"isValid")
      .select($"videoId", $"rawPrediction").collect())
    val vidPredictionMBroad = sc.broadcast(vidPredictionM)
    println("vidPredictionM size:" + vidPredictionM.size)
    println(vidPredictionM.head)

    println("#rcmds...")
    val userFactors = spark.read.parquet(cmd.getOptionValue("user_feature"))
    val userFactorsNorm = normalizer.transform(userFactors.withColumn("featuresVec", denVec($"features")))
    val useritemratingData = spark.read.parquet(cmd.getOptionValue("user_item_rating"))
    useritemratingData.printSchema()
    useritemratingData.show(2)
    itemSimsData.printSchema()
    itemSimsData.show(2)
    userFactorsNorm.printSchema()
    userFactorsNorm.show(2)

    val userPrefData = useritemratingData
      .join(itemSimsData.cache(), Seq("idtype"), "left")
      .groupBy($"userIdStr").agg(collect_list($"srcSvidwts").as("srcSvidwtsList"))
      .join(userFactorsNorm, Seq("userIdStr"), "left")

    userPrefData.printSchema()
    userPrefData.show(2 ,false)

    val rcmdData = userPrefData.repartition(8000).withColumn("rcmds", rcmd(itemFactorsMBroadcast, vidPredictionMBroad)($"userIdStr", $"normFeatures", $"srcSvidwtsList"))
      .filter($"rcmds".isNotNull)

    rcmdData.select("rcmds").repartition(200).write.text(cmd.getOptionValue("output"))


  }
}