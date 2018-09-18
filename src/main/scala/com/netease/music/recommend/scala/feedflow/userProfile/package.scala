package com.netease.music.recommend.scala.feedflow

import org.apache.spark.sql.functions._

package object userProfile {

  val usefulPage = Seq("recommendvideo", "mvplay", "videoplay", "video_classify")
  val usefulVType = Seq("mv", "video")
  val usefulresourcetype = Seq("mv", "video", "concert", "eventactivity", "song", "list", "djradio", "album")
  val filterSetForGoodClick = Seq[String]("zan", "share", "notlike", "report", "unsubscribe", "unzan_cmmt", "unzan_vdo", "unzan")
  val actionSetForBadClick = Seq[String]("notlike", "report"
    //                                           ,"unsubscribe", "unzan_cmmt", "unzan_vdo", "unzan"
  )
  val uselessSearchAction = Seq[String]("search_total", "search_skw")

  case class Video(videoId:Long, vType:String)
  case class Action(videoId:Long, vType:String, actionUserId:String, logDate:String)
  case class Impress(videoId:Long, vType:String, actionUserId:String, logDate:String, page:String, groupId:Long)
  case class ClickAction(videoId:Long, vType:String, actionUserId:String, logDate:String, actionType:String, groupId:Long)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long, logDate:String, playType:String, source:String, groupId:Long)


  // udf
  def getPrefSource = udf((page:String, source:String) => {
    if (page != null)
      page
    else if (source != null)
      source
    else
      "unknown"
  })

  def emptySeqString = udf(() => {
    Seq[String]()
  })

  def getPrefBySourceCnt = udf((sourceCnt:Int, prefOriginal:Double) => {
    if (sourceCnt <= 1)
      prefOriginal / 2
    else
      prefOriginal
  })

  def setSize = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      0
    else
      set.size
  })

  def setContain(str:String) = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      false
    else
      set.contains(str)
  })

  def setContainContain(str:String) = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      false
    else {
      var ret = false
      set.foreach{item =>
        if(item.contains(str))
          ret = true
      }
      ret
    }
  })
}
