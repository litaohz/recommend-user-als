package com.netease.music.recommend.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf

import scala.collection.{Seq, mutable}
import scala.collection.mutable.{HashMap, Set}

package object feedflow {

  object RealtimeAlgConstant {
    val ARTIST = "artist"
    val SONG = "song"
  }

  val musicVideoTags = Array(
//    "5100:音乐"
//    ,
    "1100:音乐现场"
    ,"3103:流行乐"
    ,"4100:翻唱"
//    ,"1101:舞蹈"
    ,"4104:电音"
    ,"2104:民谣"
    ,"7101:弹唱"
    ,"4107:说唱"
    ,"4105:摇滚"
    ,"4110:古风"
    ,"3109:街舞"
    ,"5102:beatbox"
    ,"3110:宅舞"
    ,"2107:阿卡贝拉"
  )

  // case class
  case class TagMap(tagId:String, tagName:String)
  case class VideoTagInfo(videoId:Long, tagIds:String, tagNames:String, tagIdAndNames:String)


  // functions
  def isMusicVideo(musicVideoCategoriesSet: mutable.Set[String]) = udf((eventCategory: String) => {

    val category1 = eventCategory.split("_")(0) + "_"
    if (musicVideoCategoriesSet.contains(category1) || musicVideoCategoriesSet.contains(eventCategory))
      1
    else
      0
  })

  def isMusicVideoByTag(musicVideoTags:Array[String]) = udf((groupIdAndNames:String) => {

    var isMusicVideo = 0
    if (groupIdAndNames == null)
      isMusicVideo
    else {
//      groupIdAndNames.split("_tab_").foreach(tag => {
//        if(isMusicVideo == 0 && musicVideoTags.contains(tag))
//          isMusicVideo = 1
//      })
      musicVideoTags.foreach(tag => {
        if(isMusicVideo == 0 && groupIdAndNames.contains(tag))
          isMusicVideo = 1
      })
      isMusicVideo
    }
  })

  def isMusicVideoByGroupId = udf((groupIdAndNames:String) => {

    var isMusicVideo = 0
    if (groupIdAndNames != null && groupIdAndNames.contains("12102:只推首页"))
      isMusicVideo = 1

    isMusicVideo
  })

  def isCertifiedCreator(certifiedUserS:collection.immutable.Set[Long]) = udf((creatorId:Long) => {

    var certifiedUser = 0
    if (certifiedUserS != null && certifiedUserS.contains(creatorId))
      certifiedUser = 1

    certifiedUser
  })

  def isMusicVideoByTagInfo(musicVideoTags:Array[String]) = udf((groupIdAndNames:String) => {

    var isMusicVideo = "null"
    if (groupIdAndNames == null)
      isMusicVideo
    else {
      musicVideoTags.foreach(tag => {
        if(isMusicVideo.equals("null") && groupIdAndNames.contains(tag))
          isMusicVideo = tag
      })
      isMusicVideo
    }
  })

  def isControversialVideo(controversialArtistIdSet:mutable.Set[String]) = udf((artistIds:String, isControversialFromEvent:Int) => {

    var ret = 0
    if (isControversialFromEvent == 1)
      ret = 1
    else {
      if (!artistIds.equals("0")) {
        for (artistId <- artistIds.split("_tab_")) {
          if (controversialArtistIdSet.contains(artistId))
            ret = 1
        }
      }
    }
    ret
  })

  def getGroups(broadcaseTagGroupM:Broadcast[HashMap[String, Set[String]]]) = udf((tagNameStr:String) => {

    val videoGroupsS = Set[String]()
    tagNameStr.split("_tab_").foreach { tag =>
      if (broadcaseTagGroupM.value.contains(tag)) {
        val groups = broadcaseTagGroupM.value.get(tag).get
        videoGroupsS ++= groups
      }
    }
    if (videoGroupsS.isEmpty)
      "null"
    else
      videoGroupsS.mkString("_tab_")
  })

  def getGroupIdAndNames(broadcaseTagGroupM:Broadcast[HashMap[Long, Seq[String]]]) = udf((tagIdStr:String) => {

    val videoGroupsS = Set[String]()
    tagIdStr.split("_tab_").foreach { tagIdStr =>
      val tagId = tagIdStr.toLong
      if (broadcaseTagGroupM.value.contains(tagId)) {
        val groupIdAndNames = broadcaseTagGroupM.value.get(tagId).get
        videoGroupsS ++= groupIdAndNames
      }
    }
    if (videoGroupsS.isEmpty)
      "null"
    else
      videoGroupsS.mkString("_tab_")
  })

  def mergeTagIds = udf((userTagIds:String, auditTagIds:String) => {

    val videoTagsS = Set[String]()
    userTagIds.split("_tab_").foreach{ tag => videoTagsS.add(tag) }
    auditTagIds.split("_tab_").foreach{ tag => videoTagsS.add(tag) }
    //去除0
    videoTagsS.remove("0")

    if (videoTagsS.isEmpty)
      "null"
    else
      videoTagsS.mkString("_tab_")
  })

  def getCtrFromStr = udf((ctrStr:String) => {
    ctrStr.split(":")(0).toFloat
  })

  def getImpressCntFromStr = udf((ctrStr:String) => {
    ctrStr.split(":")(1).toFloat
  })

  def getPlayCntFromStr = udf((ctrStr:String) => {
    ctrStr.split(":")(2).toFloat
  })

  def getSmoothCtrFromStr(alpha:Long, beta:Long) = udf((ctrStr:String) => {
    val impressCnt = ctrStr.split(":")(1).toDouble
    val playCnt = ctrStr.split(":")(2).toDouble
    ((playCnt + beta)/(impressCnt + alpha)).formatted("%.4f").toDouble
  })

  def cutTop(ceiling:Int) = udf((score:Double) => {
    Math.min(ceiling, score)
  })

  def dotVectors(weights:Array[Double]) = udf((features:org.apache.spark.ml.linalg.DenseVector) => {
    val featureBv1 = new breeze.linalg.DenseVector(features.toArray)
    val weightBv2 = new breeze.linalg.DenseVector(weights)
    featureBv1 dot weightBv2
  })

  def isValidMV = udf((typeStr:String, subTypeStr:String, duration:Long, resolution:String) => {

    val cleanType = typeStr.trim

    val subTypeSet = subTypeStr.trim.split(";")

    val typeValid = if (
      (cleanType.equals("官方版") && (subTypeSet.contains("完整版")
        || subTypeSet.contains("舞蹈版")))
        || cleanType.equals("现场版")
        || cleanType.equals("原声")
        || cleanType.equals("演唱会")
    ) {

      true
    } else {
      false
    }

    /*val resolutionValid = if (resolution.endsWith("720") || resolution.endsWith("1080")) {
      true
    } else {
      false
    }*/

    typeValid
    //    && resolutionValid
    //    && (duration > 120000)

  })

  def isValidMV_new = udf((typeStr:String, subTypeStr:String, duration:Long, resolution:String) => {

    //val cleanType = typeStr.trim
    //val subTypeSet = subTypeStr.trim.split(";")

    val typeValid = if (/*subTypeStr.contains("官方版")
      || subTypeStr.contains("演唱会")
      || subTypeStr.contains("现场版")
      || subTypeStr.contains("原声")
      || */typeStr.contains("官方版")
      || typeStr.contains("演唱会")
      || typeStr.contains("现场版")
      || typeStr.contains("原声")
    ) {

      true
    } else {
      false
    }

    typeValid
  })

  def setContain(str:String) = udf((seq:Seq[String]) => {
    if (seq == null || seq.isEmpty)
      false
    else
      seq.contains(str)
  })

  def isinSet(set: Set[String]) = udf((value:String) => {
    set.contains(value)
  })

  def splitAndGet(delim:String, index:Int) = udf((col:String) => {
    col.split(delim)(index)
  })

  def getExistsPathUnderDirectory(inputDir:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (status <- fs.listStatus(new Path(inputDir))) {
      val input = status.getPath.getParent + "/" + status.getPath.getName + "/parquet"
      set.add(input)
    }
    set
  }

  def getExistsPath(inputDir:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (status <- fs.listStatus(new Path(inputDir))) {
      val input = status.getPath.getParent + "/" + status.getPath.getName
      set.add(input)
    }
    set
  }

  def getExistsPathUnderDirectory(inputDir:String, subSubDir:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (status <- fs.listStatus(new Path(inputDir))) {
      val input = status.getPath.getParent + "/" + status.getPath.getName + "/" + subSubDir
      set.add(input)
    }
    set
  }

  def splitByDelim(delim: String) = udf((str: String) => {

    str.split(delim)
  })

  def itemCnt = udf((str: String) => {

    str.split(",").length
  })


  def isSetContainInLine(set:collection.immutable.Set[String]) = udf((line:String) => {
    var isValid = false
    line.split(",").foreach{item =>
      if (!isValid) {
        val info = item.split(":")
        val songId = info(0)
        if (set.contains(songId))
          isValid = true
      }
    }
    isValid
  })

  def splitByDelimConsiderNull(delim: String) = udf((str: String) => {

    if (str != null)
      str.split(delim)
    else
      Seq().toArray[String]
  })
}
