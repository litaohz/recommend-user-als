package com.netease.music.recommend.scala.feedflow.follow

import com.netease.music.recommend.scala.feedflow.follow.MixedSimUserNormDot.getValidUsers
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_list, _}

import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse



/**
  * Created by hzlvqiang on 2018/4/4.
  */
object Follow2Triple {

  case class IdWt(id:String, wt:Double)

  def getFriendCntM(rows: Array[Row]) = {
    val friendCntM = mutable.HashMap[String, Long]()
    for (row <- rows) {
      // "userId", "friendId"
      val friendId = row.getAs[String]("friendId")
      val userCnt = row.getAs[Long]("userCnt")
      friendCntM.put(friendId, userCnt)
    }
    friendCntM
  }

  def getM(validFriendData: Array[Row]) = {
    val idCntM = mutable.HashMap[String, Int]()
    for( row <- validFriendData) {
      val friendId = row.getAs[String]("friendIdStr")
      val cnt = row.getAs[Long]("userCnt")
      idCntM.put(friendId, cnt.toInt)
    }
    idCntM
  }

  def getFriendWts(allFriendFansCntMBroad: Broadcast[mutable.HashMap[String, Int]], allUserCnt: Long) = udf((friends:Seq[String]) => {
    val frientWts = mutable.ArrayBuffer[IdWt]()
    for (fid <- friends) {
      val cnt = allFriendFansCntMBroad.value.getOrElse(fid, -1)
      if (cnt > 0) {
        val idf = Math.log(allUserCnt / (cnt + 1.0))
        frientWts.append(IdWt(fid, idf))
      }
    }
    val sortedFwts:mutable.ArrayBuffer[IdWt] = frientWts.sortWith(_.wt > _.wt)
    if (sortedFwts.length >= 3) {
      val startPoint = sortedFwts.length / 2
      sortedFwts.remove(startPoint, sortedFwts.length - startPoint)
    }
    val result = mutable.ArrayBuffer[String]()
    for (idwt <- sortedFwts) {
      result.append(idwt.id + "\t" + idwt.wt.toString)
    }
    if (result.isEmpty) {
      null
    } else {
      result
    }
  })

  def getPubUids(rows: Array[Row]) = {
    val pubuids = mutable.HashSet[String]()
    for (row <- rows) {
      val pubuid = row.getAs[Long]("pubUid").toString
      pubuids.add(pubuid)
    }
    pubuids
  }

  def defaultRating(rating:Double) = udf( ()=>{
    rating
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("Music_Follow", true, "Music_Follow")
    options.addOption("Music_VideoRcmdMeta", true, "Music_VideoRcmdMeta")
    options.addOption("whitelist_user", true, "input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    import spark.implicits._


    val videoRcmdMetaTable = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
      .withColumn("pubUid", $"pubUid")
    val pubuids = getPubUids(videoRcmdMetaTable.collect())
    val pubuidsBroad = sc.broadcast(pubuids)

    // 云音乐福利, 网易UFO丁磊, 云音乐小秘书, 云音乐曲库, 云音乐视频酱
    /*
    val filteredUsers = mutable.HashSet[String]("2082221", "48353", "9003", "2608402", "1305093793", "1")
    println("filteredUsers size:" + filteredUsers.size)
    val filteredUsersBroad = sc.broadcast(filteredUsers)
    */
    val validUsers = sc.broadcast(getValidUsers(spark.read.json(cmd.getOptionValue("whitelist_user")).collect()))
    println("validUser id num:" + validUsers.value.size)

    val followDataPath = cmd.getOptionValue("Music_Follow")
    println("followDataPath:" + followDataPath)
    val followData = spark.read.textFile(followDataPath).map(line => {
      // ID,UserId,FriendId,CreateTime,Mutual
      val ts = line.split("\t")
      val userId = ts(1)
      val friendId = ts(2)
      if (!validUsers.value.contains(friendId) /*|| !pubuidsBroad.value.contains(friendId)*/) { // 限定在特定目标friendid里面
        (userId, "0")
      } else {
        (userId, friendId)
      }
    }).filter(!_._2.equals("0"))
      .toDF("userIdStr", "friendIdStr")
    // 选取有足够粉丝的大号
    /*
    val validFriendData = followData
      .groupBy($"friendIdStr")
      .agg(count($"userIdStr").as("userCnt"))
      // .filter($"userCnt" > 200)
      .select("friendIdStr", "userCnt")
      .filter($"userCnt" < 5000000 && $"userCnt" > 100) // 过滤过大过小的号

    val allFriendFansCntM = getM(validFriendData.collect())
    val allFriendFansCntMBroad = sc.broadcast(allFriendFansCntM)
    println("allFriendFansCntM size:" + allFriendFansCntM.size)
    */
    // tf-idf对大号加权
    val userFirendsData = followData.groupBy("userIdStr")
      .agg(collect_list($"friendIdStr").as("friends"))
    val userIdMapping = userFirendsData.select($"userIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2.toInt)
    }).toDF("userIdStr", "userId").cache()

    /*
    val allUserCnt = userIdMapping.count()

    val userFirendWtsData =  userFirendsData.withColumn("topFrirendWts", getFriendWts(allFriendFansCntMBroad, allUserCnt)($"friends"))
      .filter($"topFrirendWts".isNotNull)
    */
    userIdMapping.write.parquet(cmd.getOptionValue("output") + "/userIdMapping")

    val itemMapping = followData.select($"friendIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("friendIdStr"), line._2.toInt)
    }).toDF("friendIdStr", "friendId").cache()
    itemMapping.write.parquet(cmd.getOptionValue("output") + "/friendMapping")

    println("rating data...")
    val userItemRating =
      /*
      userFirendsData.flatMap(row => {
      val result = mutable.ArrayBuffer[(String, String, Double)]()
      val userIdStr = row.getAs[String]("userIdStr")
      val topFrirendWts = row.getAs[mutable.WrappedArray[String]]("topFrirendWts")
      for (idwts <- topFrirendWts) {
        val idwt = idwts.split("\t")
        result.append((userIdStr, idwt(0), idwt(1).toDouble))
      }
      result
    })
    */
    followData.withColumn("rating", defaultRating(1.0)())
      //.toDF("userIdStr", "friendIdStr", "rating")
      .join(userIdMapping, Seq("userIdStr"), "left")
      .join(itemMapping, Seq("friendIdStr"), "left")
      .filter($"friendId".isNotNull && $"userId".isNotNull && $"rating".isNotNull)
    println("sample...")

    userItemRating.write.parquet(cmd.getOptionValue("output") + "/data")

    /*
    val Array(training, test) = userItemRating.randomSplit(Array(0.95, 0.05))
    println("training...")
    val als = new ALS()
      .setMaxIter(5)
      .setNumUserBlocks(2000)
      .setNumItemBlocks(200)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(100)
      .setUserCol("userId")
      .setItemCol("friendId")
      .setRatingCol("rating")
    val model = als.fit(training)

    als.save(cmd.getOptionValue("output") + "/model")

    val predictions = model.transform(test)
    println("sample...")
    predictions.show(10, false)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    */
    /*
    val firendCntM = getFriendCntM(followData.groupBy("friendId").agg(count("userId").as("userCnt")).collect())
    println("firendCntM size:" + firendCntM.size)

    val allUserCnt = followData.select("userId").distinct().count()
    */

  }
}
