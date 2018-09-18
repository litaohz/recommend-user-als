package com.netease.music.recommend.scala.feedflow.follow

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{collect_list, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  *
  * 将用户对视频的互动，转化为用户和发布账号的关系
  * Created by hzlvqiang on 2018/4/4.
  */
object Follow2TripleForVideo {

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

  def getFriendWts(allFriendFansCntMBroad: Broadcast[mutable.HashMap[String, Int]], allUpuidCntMBroad: Broadcast[mutable.HashMap[String, Int]],
                   allUserCnt: Long) = udf((friends:Seq[String], posuids: Seq[String]) => {
    // val frientWts = mutable.ArrayBuffer[IdWt]()
    val frienDWtM = mutable.HashMap[String, Double]()
    if (friends != null) {
      for (fid <- friends) {
        val cnt = allFriendFansCntMBroad.value.getOrElse(fid, -1)
        if (cnt > 0) {
          val idf = Math.log(allUserCnt / (cnt + 1.0))
          frienDWtM.put(fid, idf)
        }
      }
    }
    if (posuids != null) {
      val fidCntM = mutable.HashMap[String, Int]()
      var max = 0
      for (fid <- posuids) {
        val cnt = fidCntM.getOrElse(fid, 0)
        fidCntM.put(fid, cnt + 1)
        if (fidCntM.apply(fid) > max) {
          max = fidCntM.apply(fid)
        }
      }

      for ((fid, userPrefcnt) <- fidCntM) {
        val cnt = allUpuidCntMBroad.value.getOrElse(fid, -1)
        if (cnt > 0) {
          val idf = Math.log(allUserCnt / (cnt + 1.0))
          val lstWt = frienDWtM.getOrElse(fid, 0.0)
          frienDWtM.put(fid, lstWt + 10.0 * userPrefcnt/max * idf + 1.0)// cnt 最大为10 TODO
          // frientWts.append(IdWt(fid, 10 * cnt / max * idf))
        }
      }
    }

    val sortedFwts = frienDWtM.toArray[(String, Double)].sortWith(_._2 > _._2)
    val result = mutable.ArrayBuffer[String]()
    for (i <- 0 until sortedFwts.size if i < sortedFwts.size * 3 / 4) {
      val idwt = sortedFwts(i)
      result.append(idwt._1 + "\t" + idwt._2.toString)
    }
    if (result.isEmpty) {
      null
    } else {
      result
    }
  })

  def getPubUids(rows: Array[Row]) = {
    val pubuids = mutable.HashSet[String]()
    val vidPubidM = mutable.HashMap[String, String]()
    for (row <- rows) {
      val vid = row.getAs[Long]("videoId").toString
      val pubuid = row.getAs[Long]("pubUid").toString
      pubuids.add(pubuid)
      vidPubidM.put(vid, pubuid)
    }
    (pubuids, vidPubidM)
  }

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
    options.addOption("vid_uid_pos", true, "input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    import spark.implicits._


    val videoRcmdMetaTable = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
      .withColumn("pubUid", $"pubUid")
    val (pubuids, vidPubuids) = getPubUids(videoRcmdMetaTable.collect())
    val pubuidsBroad = sc.broadcast(pubuids)
    val vidPubidBroad = sc.broadcast(vidPubuids)

    // 云音乐福利, 网易UFO丁磊, 云音乐小秘书, 云音乐曲库, 云音乐视频酱
    val filteredUsers = mutable.HashSet[String]("2082221", "48353", "9003", "2608402", "1305093793", "1")
    println("filteredUsers size:" + filteredUsers.size)
    val filteredUsersBroad = sc.broadcast(filteredUsers)

    val followDataPath = cmd.getOptionValue("Music_Follow")
    println("followDataPath:" + followDataPath)
    val followData = spark.read.textFile(followDataPath).map(line => {
      // ID,UserId,FriendId,CreateTime,Mutual
      val ts = line.split("\t")
      val userId = ts(1)
      val friendId = ts(2)
      if (!filteredUsersBroad.value.contains(friendId) && pubuidsBroad.value.contains(friendId)) {
        (userId, friendId)
      } else {
        (userId, "0")
      }
    }).filter(!_._2.equals("0"))
      .toDF("userIdStr", "friendIdStr")


    // 视频用户正向数据
    val userPosVideosPrefData = spark.read.textFile(cmd.getOptionValue("vid_uid_pos")).map(line => {
      val ts = line.split("\t")
      val vid = ts(0)
      var uid = ts(1).toLong
      if (uid < 0) {
        uid = -uid
      }
      val puvid = vidPubidBroad.value.getOrElse(vid, "")
      var res = ("", "")
      if (puvid != "") {
        res = (uid.toString, puvid)
      }
      res
    }).toDF("userIdStr", "friendIdStr")
      .filter($"userIdStr" =!= "")
    val validUserPubuserData = userPosVideosPrefData.groupBy("friendIdStr").agg(count("userIdStr").as("userCnt"))
      .filter($"userCnt" < 5000000 && $"userCnt" > 50)
    val validUpuserFCntM = getM(validUserPubuserData.collect())
    val validUpuserFCntMBroad = sc.broadcast(validUpuserFCntM)
    println("validUpuserFCntM size:" + validUpuserFCntM.size)

    // 选取有足够粉丝的大号
    val validFriendData = followData
      .groupBy($"friendIdStr")
      .agg(count($"userIdStr").as("userCnt"))
      // .filter($"userCnt" > 200)
      .select("friendIdStr", "userCnt")
      .filter($"userCnt" < 5000000 && $"userCnt" > 50) // 过滤过大过小的号
    val allFriendFansCntM = getM(validFriendData.collect())
    val allFriendFansCntMBroad = sc.broadcast(allFriendFansCntM)
    println("allFriendFansCntM size:" + allFriendFansCntM.size)



    // tf-idf对大号加权
    val userFirendsData = followData.groupBy("userIdStr")
      .agg(collect_list($"friendIdStr").as("friends"))
    val userPosData = userPosVideosPrefData.groupBy("userIdStr")
      .agg(collect_list($"friendIdStr").as("posUpids"))


    val userIdMapping = userFirendsData.select($"userIdStr")
      .union(userPosData.select($"userIdStr"))
      .rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2.toInt)
    }).toDF("userIdStr", "userId").cache()

    val allUserCnt = userIdMapping.count()

    val userFirendWtsData =  userFirendsData.join(userPosData, Seq("userIdStr"), "outer")
      .withColumn("topFrirendWts", getFriendWts(allFriendFansCntMBroad, validUpuserFCntMBroad, allUserCnt)($"friends", $"posUpids"))
      .filter($"topFrirendWts".isNotNull)


    userIdMapping.write.parquet(cmd.getOptionValue("output") + "/userIdMapping")

    val itemMapping = validFriendData.select($"friendIdStr")
        .union(validUserPubuserData.select("friendIdStr"))
      .rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("friendIdStr"), line._2.toInt)
    }).toDF("friendIdStr", "friendId").cache()
    itemMapping.write.parquet(cmd.getOptionValue("output") + "/friendMapping")

    println("rating data...")
    val userItemRating = userFirendWtsData.flatMap(row => {
      val result = mutable.ArrayBuffer[(String, String, Double)]()
      val userIdStr = row.getAs[String]("userIdStr")
      val topFrirendWts = row.getAs[mutable.WrappedArray[String]]("topFrirendWts")
      for (idwts <- topFrirendWts) {
        val idwt = idwts.split("\t")
        result.append((userIdStr, idwt(0), idwt(1).toDouble))
      }
      result
    }).toDF("userIdStr", "friendIdStr", "rating")
      .join(userIdMapping, Seq("userIdStr"), "left")
      .join(itemMapping, Seq("friendIdStr"), "left")
      .filter($"friendId".isNotNull && $"userId".isNotNull && $"rating".isNotNull)
    println("sample...")

    userItemRating.write.parquet(cmd.getOptionValue("output") + "/data")

  }
}
