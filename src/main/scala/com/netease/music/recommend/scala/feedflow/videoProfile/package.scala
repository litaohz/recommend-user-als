package com.netease.music.recommend.scala.feedflow

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf

package object videoProfile {


  val usefulPage = Seq("recommendvideo", "mvplay", "videoplay", "video_classify")
  val usefulSource = Seq("mvplay_recommendvideo", "videoplay_recommendvideo", "recommendvideo", "video_classify")
  val usefulVType = Seq("mv", "video")

  // case classes
  case class VideoResolution(videoId:Long, resolution:Int, duration:Int, width:Int, height:Int, resolutionInfo:String)
  case class ShareAction(videoId:Long, vType:String, actionUserId:Long, position:Int, shareType:String, os:String)


  def getClickratetypeFromPage = udf((page:String) => {
    if (page.equals("mvplay") || page.equals("videoplay"))
      "detail"
    else if (page.equals("video_classify"))
      "classify"
    else
      "flow"
  })

  def getClickratetypeFromSource = udf((source:String) => {
    if (source.equals("mvplay_recommendvideo") || source.equals("videoplay_recommendvideo"))
      "detail"
    else if (source.equals("video_classify"))
      "classify"
    else
      "flow"
  })


  // functions
  def getExistsPath(inputPaths:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (input <- inputPaths.split(",")) {
      if (fs.exists(new Path(input))) {
        set.add(input)
      }
    }
    set
  }

  def getClickrate = udf((impressCnt:Long, actionCnt:Long) => {
    if (impressCnt == 0)
      "0.000:0:0"
    else
      (actionCnt.toDouble/impressCnt.toDouble).formatted("%.4f") + ":" + impressCnt + ":" + actionCnt
  })

  def getPositionClickrate = udf((impressCnt:Long, actionCnt:Long, position:Integer) => {
    if (impressCnt == 0)
      "0.000:0:0:0"
    else
      (actionCnt.toDouble/impressCnt.toDouble).formatted("%.4f") + ":" + impressCnt + ":" + actionCnt + ":" + position
  })

  def getPositionClickrateList = udf((seq:Seq[Seq[String]]) => {
    val posClickrateM = collection.mutable.Map[Integer, Array[Long]]()
    seq.foreach {line =>
      line.foreach {item =>
        val info = item.split(":")
        val impressCnt = info(1).toLong
        val actionCnt = info(2).toLong
        val position = info(3).toInt
        if (posClickrateM.contains(position)) {
          val clickrateInfo = posClickrateM.get(position).get
          clickrateInfo(0) += impressCnt
          clickrateInfo(1) += actionCnt
          posClickrateM.put(position, clickrateInfo)
        } else {
          posClickrateM.put(position, Array(impressCnt, actionCnt))
        }
      }
    }
    if (posClickrateM.size > 0) {
      posClickrateM.toArray.sortWith(_._1 < _._1)
        .map(line => {
          val clickrate = if (line._2(0) <= 0) "0.0000" else (line._2(1).toDouble / line._2(0).toDouble).formatted("%.4f")
          clickrate + ":" + line._2.mkString(":") + ":" + line._1
        }).mkString(",")
    } else {
      "0.0000:0:0:0"
    }
  })

  def getPlayendrate = udf((playCnt:Long, playendCnt:Int) => {
    if (playCnt == 0)
      "0.000:0:0"
    else
      (playendCnt.toDouble/playCnt.toDouble).formatted("%.7f") + ":" + playCnt + ":" + playendCnt
  })

  def getPlayratio = udf((playCnt:Long, playTime:Long, duration:Int) => {
    if (playCnt == 0 || duration == 0)
      "0.000:0:0"
    else {
      var playratio = playTime.toDouble / (playCnt * duration).toDouble
//      if (playratio > 1)
//        playratio = 1.0
      playratio.formatted("%.7f") + ":" + playCnt + ":" + duration
    }
  })

  def getMajorityIntValue = udf((list:Seq[Int]) => {

    if (list.isEmpty)
      0
    else {
      val valueCntM = collection.mutable.Map[Int, Int]()
      list.foreach { value =>
        val cnt = valueCntM.getOrElse(value, 0) + 1
        valueCntM.put(value, cnt)
      }
      if (valueCntM.size == 1)
        list(0)
      else {
        var maxCnt = 0
        var returnValue = 0
        valueCntM.foreach{entry =>
          if (maxCnt < entry._2) {
            maxCnt = entry._2
            returnValue = entry._1
          }
        }
        returnValue
      }
    }
  })

  def getCorrectPlaytime = udf((time:Long, maxDuration:Int) => {

    var timeCorrected = time;
    if (time < 0)
      timeCorrected *= -1
    if (timeCorrected > maxDuration)
      timeCorrected = maxDuration
    timeCorrected
  })

  def smoothPlaytimePerUser = udf((playtimePerUser:Long, maxDuration:Int) => {

    math.min(playtimePerUser, 2*maxDuration)
  })

  def isHot = udf((alg:String) => {

    if (alg.toLowerCase.contains("hot"))
      1
    else
      0
  })

  def getCtrFromStr = udf((ctrStr:String) => {
    ctrStr.split(":")(0).toFloat
  })
}
