package com.netease.music.recommend.scala.feedflow.utils

import com.netease.music.recommend.scala.feedflow.artist.{OnlineVideoInfo, OnlineVideoInfoNew}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._

import scala.collection.{Set, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.Random

object userFunctions {

  def getNewStringColumn(defaultValue: String) = udf((id: Long) => {

    defaultValue
  })

  def extractByPos(idPos:Int, delim:String) = udf((videoInfo:String) => {
    videoInfo.split(delim)(idPos)
  })

  def splitByDelim(delim: String) = udf((artistIdsForRec: String) => {

    artistIdsForRec.split(delim)
  })

  def expandVector = udf((vec:org.apache.spark.ml.linalg.Vector) => {
    var resArr = vec.toArray
    var continue = true
    while (continue) {
      var smallNumberCnt = 0
      resArr.foreach { value => {
        if (value <= 0.009)
          smallNumberCnt += 1
      }
      }
      val smallNumberRatio = smallNumberCnt / vec.size
      if (smallNumberRatio<= 0.2)
        continue = false
      resArr = vec.toArray.map(_ * 1000)
    }
    org.apache.spark.ml.linalg.Vectors.dense(resArr)
  })

  def hasBigDimVector(bigDigitNumThred:Int) = udf((vec:Seq[Float]) => {
    var hasBigDim = 0
    var bigDigitNum = 0
    vec.foreach { value => {
      if (value > 0.01 || value < 0.01) {
        bigDigitNum += 1
      }
    }
    }
    if (bigDigitNum >= bigDigitNumThred)
      hasBigDim = 1
    hasBigDim
  })

  def double2Long = udf((id:Double) => {
    id.toLong
  })

  def set2String = udf((set:Seq[Long]) =>{
    set.mkString("-")
  })

  def getFeatureVector = udf((features:Seq[Float]) => {

    Vectors.dense(features.map(_.toDouble).toArray)
  })

  def getFinalPref = udf((pref: Float) => {
    var final_pref = pref
    if (pref == 0) {
      val r = new Random()
      if(r.nextFloat()<0.2) {
        final_pref = pref - 2
      }
    }
    final_pref
  })

  def string2int = udf((col: String) => {
    col.toFloat.toInt
  })

  def int2double = udf((col: Int) => {
    col.toDouble
  })

  def string2long = udf((col: String) => {
    col.toLong
  })


  //  def isControversialVideo = udf((controversialArtistidForMarking: Long) => {
//    if (controversialArtistidForMarking > 0)
//      1
//    else
//      0
//  })

  def isControversialEvent = udf((isControversialEvent: Int, isControversialFigure: Int) => {

    if (isControversialEvent == 1 || isControversialFigure == 1)
      1
    else
      0
  })

  def getDefaultColumn(defaultValue: String) = udf(() => {
    defaultValue
  })

  def getDefaultColumn(defaultValue: Int) = udf(() => {
    defaultValue
  })

  def collectVideoByKey(key:String, videoInfos:Iterable[String]):String = {

    key + "\t" + videoInfos.mkString(",")
  }

  def collectOnlineVideoByKey(key:String, videoInfos:Iterable[OnlineVideoInfo]):String = {

    key + "\t" + videoInfos.toArray
      .sortWith(_.rawPrediction > _.rawPrediction)
      .map(info => info.videoId + ":" + info.vType + ":" + info.alg + ":" + info.rawPrediction.formatted("%.4f") + ":" + info.recallType)
      .mkString(",")
  }

  def collectOnlineVideoByKeyNew(key:String, videoInfos:Iterable[OnlineVideoInfoNew]):String = {

    key + "\t" + videoInfos.toArray
      .sortWith(_.rawPrediction > _.rawPrediction)
      .map(info => info.videoInfo)
      .mkString(",")
  }

  def reduceVideoByType(key:Tuple2[String, String], values:Iterable[Tuple4[Int, String, String, Double]]):(String, Int, String, String, Double)= {

    var typeVideoM = Map[Int, Tuple3[String, String, Double]]()
    values.foreach{tuple =>
      typeVideoM += (tuple._1 -> (tuple._2, tuple._3, tuple._4))
    }

    var tuple = typeVideoM.getOrElse(0, null)
    var recallType = 0
    if (tuple == null) {
      tuple = typeVideoM(1)
      recallType = 1
    }
    (key._1, recallType, tuple._1, tuple._2, tuple._3)
  }

  def getUserLanguageBlacklist(userLanguagePrefSet: Seq[Int]): Set[Int] = {

    var blackLanguagePrefSet = (2 to 5).toSet[Int] // 去除0，1，6（无、中文、全部）
    val userLanguageOldPrefSet = mutable.Set[Int]()
    userLanguagePrefSet.foreach(languageNew => {
      if (languageNew == 96)
        userLanguageOldPrefSet += 2
      else if (languageNew == 16)
        userLanguageOldPrefSet += 3
      else if (languageNew == 8)
        userLanguageOldPrefSet += 4
    })
    blackLanguagePrefSet = blackLanguagePrefSet &~ userLanguageOldPrefSet // 去除用户偏好的语种
    blackLanguagePrefSet
  }

  def getSongRankNotSatisfied(userRank: Float): Set[Long] = {

    val min = math.ceil(math.max(0, userRank - 4)).toLong
    val max = math.floor(math.min(6, userRank + 4)).toLong

    (0l to 6l).toSet[Long] &~ (min to max).toSet[Long]
  }

  def getWhiteVideosForActiveUsersUsingRecall(broadcastVideoInfoM: Broadcast[mutable.HashMap[Long, Seq[Any]]]
                                             ,broadcastControversialVideoSet: Broadcast[mutable.Set[String]]
                                             ,broadcastVideoTobeExaminedSet: Broadcast[mutable.Set[String]]
                                             ,broadcastSameArtistVideoNum: Broadcast[Int]
                                             ,broadcastSameKeywordVideoNum: Broadcast[Int]
                                             ,broadcastSameCategory2elvelVideoNum: Broadcast[Int]
                                             ,broadcastMaxVideoNum: Broadcast[Int]
                                             ) = udf((recallVideos: String, recedVideos: String, prefLanguagesFromSong: Seq[Int], prefLanguagesInEvent: Seq[Int], prefSongtags: Seq[String]
                                             ,subArtists: Seq[Long], followingCreators: Seq[Long], userId: Long, userRank: Float, userGender:Int) => {

    val recedVideoSet = recedVideos.split(",").toSet
    var videoWhiteList = ListBuffer[String]()
    /*// 去除已曝光动态
    //eventSetTobeExamined = eventSetTobeExamined &~ (recedEventSet)*/

    for (videoId <- recallVideos.split(",")) {
      // 保留未曝光动态
      if (!recedVideoSet.contains(videoId)) {
        val video = broadcastVideoInfoM.value.get(videoId.toLong)
        if (video != None) {
          val creatorId = video.get(3).toString().toLong
          // 保留：
          // 1.没有关注其他用户的情况
          // 2.关注其他用户 且 当前动态创建者不在该用户的关注者列表里
          if (followingCreators == null || !followingCreators.contains(creatorId)) {
            // 保留：非用户本人发的动态的情况
            if (userId != creatorId) {
              val notSatisfiedSongRankSet = getSongRankNotSatisfied(userRank)
              val songRank = video.get(8).toString().toLong
              // 保留：songRank和userRank相差不大(相差小于4)的情况
              if (!notSatisfiedSongRankSet.contains(songRank)) {
                val language = video.get(11).toString().toInt
                val userLanguagePrefSet = {
                  if (prefLanguagesFromSong != null && prefLanguagesInEvent != null)
                    prefLanguagesFromSong ++ prefLanguagesInEvent
                  else if (prefLanguagesFromSong != null)
                    prefLanguagesFromSong
                  else if (prefLanguagesInEvent != null)
                    prefLanguagesInEvent
                  else
                    Seq[Int]()
                }
                val blackUserLanguagePrefSet = getUserLanguageBlacklist(userLanguagePrefSet)
                // 保留：符合用户语种偏好的动态
                if (!blackUserLanguagePrefSet.contains(language)) {
                  val artistId = video.get(6).toString
                  // 保留：
                  // 1.为识别出artistId的情况
                  // 2.动态相关艺人不是敏感艺人的情况
                  // 2.动态相关艺人是敏感艺人 且 用户订阅艺人列表中包含该艺人的情况
                  if (artistId == "0" || !broadcastControversialVideoSet.value.contains(videoId) || (subArtists != null && subArtists.contains(artistId))) {
                    val eventCategory = video.get(17).toString()
                    val eventCategory1 = eventCategory.split("_")(0)
                    val songTag = video.get(10).toString
                    // 保留：
                    // 1. 非音乐资讯动态 或 无songTag的动态 的情况
                    // 2. 非第一种情况 且 用户有songTag偏好 且 用户songTag偏好列表中包含动态的songTag 的情况
                    if (eventCategory1 != "1" || songTag == "null" || (prefSongtags != null && prefSongtags.contains(songTag))) {

                      /* // abtest for category2level
                       val categoryExpConf = broadcastCategoryExp.value
                       if (categoryExpConf != null && categoryExpConf.matchUid(userId)) {
                         if (!filterCategory11ByUserGender(userGender, eventCategory))
                           eventWhiteList += eventId

                       }
                       else*/
                      videoWhiteList += videoId
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    val resCntM = mutable.HashMap[String, Int]()
    val maxVideoNum = broadcastMaxVideoNum.value
    val sameArtistVideoNum = broadcastSameArtistVideoNum.value
    //val sameKeywordVideoNum = broadcastSameKeywordVideoNum.value
    val sameCategory2levelVideoNum = broadcastSameCategory2elvelVideoNum.value

    if (videoWhiteList.size > 0) {
      // var results = mutable.Seq[String]()
      val sb = new StringBuilder()
      var cnt = 0
      for (videoId <- videoWhiteList) {
        if (cnt < maxVideoNum) {
          val video = broadcastVideoInfoM.value.get(videoId.toLong)
          // "eventId", "resourceType", "resourceId", "creatorId", "eventCreateTime"
          // "songId", "artistId", "topicId", "songRank", "songArea"
          // "songTag", "language", "isControversialFigure", "score", "effective"
          // "excellent" ,"lastUpdateTime", "eventCategory", "isVerifiedEvent", "isControversialEvent"
          // "artistIdsForRec", "keywords", "isMusicEvent"
          val artistId = video.get(6).toString
          val artCnt = resCntM.getOrElse(artistId, 0)
          // 保留：已推荐artist数小于artist多样性阈值的情况
          if (artCnt < sameArtistVideoNum || artistId == "0") {
            /*val keywordStr = video.get(21).toString
            var maxKeyCnt = 0
            val keywords = keywordStr.split(",")
            if (keywordStr != "null") {
              for (keyword <- keywords) {
                if (!keyword.isEmpty) {
                  val keyCnt = resCntM.getOrElse(keyword, 0)
                  if (keyCnt > maxKeyCnt) {
                    maxKeyCnt = keyCnt
                  }
                }
              }
            }
            // 保留：已推荐keyword数小于keyword多样性阈值的情况
            if (maxKeyCnt < sameKeywordVideoNum) {*/

              val eventCategory = video.get(17).toString
              val category2levelCnt = resCntM.getOrElse(eventCategory, 0)
              // 保留：已推荐category2level数小于category2level多样性阈值的情况
              if (category2levelCnt < sameCategory2levelVideoNum || eventCategory.startsWith("0_")) {

                // 更新已推荐artist计数
                if (artistId != "0")
                  resCntM.put(artistId, 1 + artCnt)
                /*for (keyword <- keywords) {
                  if (!keyword.isEmpty) {
                    // 更新已推荐keyword计数
                    resCntM.put(keyword, resCntM.getOrElse(keyword, 0) + 1)
                  }
                }*/
                // 更新category2level计数
                if (!eventCategory.startsWith("0_"))
                  resCntM.put(eventCategory, 1 + category2levelCnt)

                val resultSeq = mutable.Seq[String](videoId, video.get(1).toString, video.get(2).toString, video.get(3).toString, video.get(5).toString,
                  video.get(6).toString, video.get(7).toString, video.get(17).toString, video.get(11).toString, "1.0")
                // output format:
                // evt_id  :  res_type  :  res_id  :  create_user_id  :  song_id  :
                // art_id  :  topic_id  :  category  : language  :  score
                sb.append(resultSeq.mkString(":")).append(",")
                cnt = cnt + 1
              }
            //}
          }
        }
      }

      sb.substring(0, sb.length - 1)

    } else {
      "null"
    }

  })


  def getWhiteVideosForActiveUsers(broadcastVideoInfoM: Broadcast[mutable.HashMap[Long, Seq[Any]]]
                                  ,broadcastControversialVideoSet: Broadcast[mutable.Set[String]]
//                                  ,broadcastVideoTobeExaminedSet: Broadcast[mutable.Set[String]]
                                  ,broadcastSameArtistVideoNum: Broadcast[Int]
                                  ,broadcastSameKeywordVideoNum: Broadcast[Int]
                                  ,broadcastSameCategory2elvelVideoNum: Broadcast[Int]
                                  ,broadcastMaxVideoNum: Broadcast[Int]
                                  ) = udf((recedVideos: String, subArtists: Seq[Long], followingCreators: Seq[Long], userId: Long) => {

    val recedVideoSet = recedVideos.split(",").toSet
//    var videoWhiteList = ListBuffer[String]()
    val sb = new StringBuilder()
    for (videoId <- broadcastVideoInfoM.value.keySet) {
      // 保留未曝光动态
      if (!recedVideoSet.contains(videoId.toString)) {
        val video = broadcastVideoInfoM.value.get(videoId.toLong)
        //"videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        //"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        //"score", "title", "description"
        if (video != None) {
          val creatorId = video.get(2).toString().toLong
          // 保留：
          // 1.没有关注其他用户的情况
          // 2.关注其他用户 且 当前动态创建者不在该用户的关注者列表里
          if (followingCreators == null || !followingCreators.contains(creatorId)) {
            // 保留：非用户本人发的动态的情况
            if (userId != creatorId) {

              val artistIds = video.get(3).toString
              var isFilteredByControversial = false
              for (artistId <- artistIds.split("_tab_")) {
                if (artistId != "0" && subArtists != null && !subArtists.contains(artistId) && broadcastControversialVideoSet.value.contains(videoId.toString)) {
                  isFilteredByControversial = true
                }
                // 保留：
                // 1.为识别出artistId的情况
                // 2.动态相关艺人不是敏感艺人的情况
                // 2.动态相关艺人是敏感艺人 且 用户订阅艺人列表中包含该艺人的情况
                if (!isFilteredByControversial)
                  sb.append(video.mkString(":")).append(",")
              }
            }
          }
        }
      }
    }
    if (sb.length > 1)
      sb.substring(0, sb.length - 1)
    else
      "null"

//    val resCntM = mutable.HashMap[String, Int]()
//    val maxVideoNum = broadcastMaxVideoNum.value
//    val sameArtistVideoNum = broadcastSameArtistVideoNum.value
//    //val sameKeywordVideoNum = broadcastSameKeywordVideoNum.value
//    val sameCategory2levelVideoNum = broadcastSameCategory2elvelVideoNum.value
//
//    if (videoWhiteList.size > 0) {
//      // var results = mutable.Seq[String]()
//      val sb = new StringBuilder()
//      var cnt = 0
//      for (videoId <- videoWhiteList) {
//        if (cnt < maxVideoNum) {
//          val video = broadcastVideoInfoM.value.get(videoId.toLong)
//          // "eventId", "resourceType", "resourceId", "creatorId", "eventCreateTime"
//          // "songId", "artistId", "topicId", "songRank", "songArea"
//          // "songTag", "language", "isControversialFigure", "score", "effective"
//          // "excellent" ,"lastUpdateTime", "eventCategory", "isVerifiedEvent", "isControversialEvent"
//          // "artistIdsForRec", "keywords", "isMusicEvent"
//          val artistId = video.get(6).toString
//          val artCnt = resCntM.getOrElse(artistId, 0)
//          // 保留：已推荐artist数小于artist多样性阈值的情况
//          if (artCnt < sameArtistVideoNum || artistId == "0") {
//            /*val keywordStr = video.get(21).toString
//            var maxKeyCnt = 0
//            val keywords = keywordStr.split(",")
//            if (keywordStr != "null") {
//              for (keyword <- keywords) {
//                if (!keyword.isEmpty) {
//                  val keyCnt = resCntM.getOrElse(keyword, 0)
//                  if (keyCnt > maxKeyCnt) {
//                    maxKeyCnt = keyCnt
//                  }
//                }
//              }
//            }
//            // 保留：已推荐keyword数小于keyword多样性阈值的情况
//            if (maxKeyCnt < sameKeywordVideoNum) {*/
//
//            val eventCategory = video.get(17).toString
//            val category2levelCnt = resCntM.getOrElse(eventCategory, 0)
//            // 保留：已推荐category2level数小于category2level多样性阈值的情况
//            if (category2levelCnt < sameCategory2levelVideoNum || eventCategory.startsWith("0_")) {
//
//              // 更新已推荐artist计数
//              if (artistId != "0")
//                resCntM.put(artistId, 1 + artCnt)
//              /*for (keyword <- keywords) {
//                if (!keyword.isEmpty) {
//                  // 更新已推荐keyword计数
//                  resCntM.put(keyword, resCntM.getOrElse(keyword, 0) + 1)
//                }
//              }*/
//              // 更新category2level计数
//              if (!eventCategory.startsWith("0_"))
//                resCntM.put(eventCategory, 1 + category2levelCnt)
//
//              val resultSeq = mutable.Seq[String](videoId, video.get(1).toString, video.get(2).toString, video.get(3).toString, video.get(5).toString,
//                video.get(6).toString, video.get(7).toString, video.get(17).toString, video.get(11).toString, "1.0")
//              // output format:
//              // evt_id  :  res_type  :  res_id  :  create_user_id  :  song_id  :
//              // art_id  :  topic_id  :  category  : language  :  score
//              sb.append(resultSeq.mkString(":")).append(",")
//              cnt = cnt + 1
//            }
//            //}
//          }
//        }
//      }



  })
}
