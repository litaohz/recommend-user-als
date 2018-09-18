package com.netease.music.recommend.scala.feedflow

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import com.netease.music.recommend.scala.feedflow.mainpage.GetMainpageRcmdResource.usefulTagtypeS
import scala.collection._

package object cf {


  val usefulCFRecalltypeS = Set[String]("0", "4", "5")
  val usefulResourcetype4MCF = Set[String]("video", "mv", "concert", "eventactivity")

  case class userItemPref(userId:Long, itemId:Long, pref:Float)


  // functions
  def cosSimilarity(attr1:Array[Float], attr2:Array[Float]):Double = {
    var x2 = 0.0
    var y2 = 0.0
    var xy = 0.0
    for (i <- 0 to (attr1.length-1)) {
      x2 += attr1(i) * attr1(i)
      y2 += attr2(i) * attr2(i)
      xy += attr1(i) * attr2(i)
    }
    xy / (math.sqrt(x2) * math.sqrt(y2))
  }


  // udfs
  def isInSet(value:Any) = udf((seq:Seq[Any]) => {
    if (seq == null || seq.isEmpty)
      false
    else
      seq.contains(value)
  })

  def stringToLong = udf((value:String) => {
    value.toLong
  })

  def vectorNorm2 = udf((array:Seq[Float]) => {
    val x2 = array.map(x => x*x).reduce(_ + _)
    math.sqrt(x2.toDouble)
  })

  def id1 = udf((id:Int) => {
    id + 1
  })

  def getGroupVideoPref = udf((viewTime:Long, playTypes:Seq[String], subscribeCnt:Long, actionTypeList:Seq[String], videoPref:Double) => {

    if (videoPref != null && videoPref < 0)
      0.0
    else {
      if (viewTime != null && viewTime > 30)
        5.0
      else {
        if (playTypes != null && playTypes.contains("playend"))
          5.0
        else {
          if (subscribeCnt != null && subscribeCnt > 0)
            5.0
          else {
//            if (actionTypeList != null && actionTypeList.size > 3)
//              5.0
//            else
              0.0
          }
        }
      }
    }
  })

  def getVideoPref(sourceType:String) = udf((viewTime:Long, playTypes:Seq[String], subscribeCnt:Long, actionTypeList:Seq[String], videoPref:Double) => {

    if (videoPref != null && videoPref < 0)
      0.0
    else {
      if (viewTime != null && viewTime > 30)
        5.0
      else {
        if (playTypes != null && playTypes.contains("playend"))
          5.0
        else {
          if (subscribeCnt != null && subscribeCnt > 0)
            5.0
          else {
//            if (sourceType.equals("video_classify") && actionTypeList != null && actionTypeList.size > 3)
//              5.0
//            else
              0.0
          }
        }
      }
    }
  })

  def getExplicitVideoPref = udf((prefList:Seq[Double]) => {

    var isNegative = false
    var prefSum = 0.0
    prefList.foreach(pref => {
      if (pref < 0)
        isNegative = true
      else {
        prefSum += pref
      }
    })

    if (isNegative)
      -5.0
    else if (prefSum == 0 && prefList.size > 5)
      -2.0
    else if (prefSum > 20)
      prefSum
    else
      0
  })

  def mergeSeq = udf((seqs:Seq[Seq[String]]) => {
//    val ret = collection.mutable.HashSet[String]()
//    if (seqs != null) {
//      seqs.foreach { seq =>
//        if (seq != null) {
//          seq.foreach{item =>
//            ret.add(item)
//          }
//        }
//      }
//    }
//    ret
    seqs.flatMap(seq => seq)
  })

  def euDistance = udf((seq:Seq[Float]) => {
    math.sqrt(seq.map(x => x * x).sum)
  })

  def cutTop(ceiling:Int) = udf((score:Double) => {
    Math.min(ceiling, score);
  })

  def cutDecret = udf((score:Double) => {
    if (score >= 70)
      8
    else {
      (score / 10).toInt + 1
    }
  })

  def filter4CFByAlg = udf((algSet:Seq[String]) => {
    var remain = true
    if (algSet == null || algSet.isEmpty)
      remain = false
    else {
      algSet.foreach { alg =>
        if (alg.equals("firstpage_force_rcmd") || alg.contains("hot") || alg.equals(""))
          remain = false
      }
    }
    remain
  })

  def ifKeepThisAction = udf((records:Seq[String]) => {

    var hasImpress = false
    var firstAlg = ""
    var returnAlg = ""
    var lastImpress = 0L
    for (record <- records if returnAlg.isEmpty) {

      try {
        var info = record.split(":")
        val alg = info(0)
        val logTime = info(1).toLong
        val recordType = info(2).toInt
        if (recordType == 0) {
          hasImpress = true
          lastImpress = logTime
          firstAlg = alg
        } else {
          if (logTime - lastImpress < 12 * 3600 * 1000 && hasImpress)
            returnAlg = firstAlg
        }
      } catch {
        case ex:Exception => println("error info:" + record)
      }
    }

    returnAlg
  })


  def mixedId2OriginalId(bc_M:Broadcast[immutable.Map[String, String]]) = udf((id:String) => {
    bc_M.value(id)
  })

  def originalId2MixedId(bc_M:Broadcast[immutable.Map[String, String]]) = udf((id:String) => {
    bc_M.value(id)
  })

  def isTooHot(hotItemSet:Broadcast[immutable.Set[String]]) = udf((id:String) => {
    hotItemSet.value.contains(id)
  })

  def filterByHotRule(bc_hotPartFilterM:Broadcast[immutable.Map[String, Seq[String]]]) = udf((id0:String, id1:String) => {
    val filteredIdS = bc_hotPartFilterM.value.getOrElse(id1, Seq())
    filteredIdS.contains(id0)
  })

  def filterByArtSongRule(bc_hotArtOrSongIdFilterS:Broadcast[immutable.Set[String]]) = udf((id1:String) => {

    bc_hotArtOrSongIdFilterS.value.contains(id1)
  })

  /*def filterByArtSongRule(bc_hotArtOrSongIdFilterS:Broadcast[Set[String]], bc_notHotArtOrSongIdFilterS:Broadcast[Set[String]]) = udf((id1:String) => {

    bc_hotArtOrSongIdFilterS.value.contains(id1) || bc_notHotArtOrSongIdFilterS.value.contains(id1)
  })*/

  def setConcat(sep:String) = udf((set:Seq[String]) => {
    set.mkString(sep)
  })

  def setSize = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      0
    else
      set.size
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

  def setContain(str:String) = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      false
    else
      set.contains(str)
  })

  def setSetContain(str:String) = udf((setS:Seq[Seq[String]]) => {
    if (setS == null || setS.isEmpty)
      false
    else
      setS.flatten.toSet.contains(str)
  })

  def setContainsItem(set:Set[String]) = udf((item:String) => {
    set.contains(item)
  })

  def setContainsLongitem(set:Set[Long]) = udf((item:Long) => {
    set.contains(item)
  })

  def splitAndGet(delim:String, index:Int) = udf((col:String) => {
    col.split(delim)(index)
  })

  def splitAndGetLast(delim:String) = udf((col:String) => {
    val info = col.split(delim)
    val index = info.length-1
    info(index)
  })

  def getVideoKwdM(videoInput: String) = {
    val videoKwdM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(videoInput)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path)) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getVideoKwdMFromFile(bufferedReader, videoKwdM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getVideoKwdMFromFile(bufferedReader, videoKwdM)
      bufferedReader.close()
    }
    videoKwdM
  }

  def getVideoKwdMFromFile(reader: BufferedReader, videoKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]]) = {
    var line: String = null
    line = reader.readLine()
    while (line != null) {
      try {
        // 1000052  \t  video  \t  薛之谦_ET,演员_ET
        val ts = line.split("\t")
        val videoId = ts(0)
        val vType = ts(1)
        val kwds = mutable.ArrayBuffer[String]()
        if (ts.length >= 3 && vType.equals("video")) {
          for (kwdType <- ts(2).split(",")) {
            val kwdtype = kwdType.split("_")
            if (kwdtype.length >= 2) {
              val kwd = kwdtype(0)
              // 获取所有以"ET", "AT", "WK", "PT", "TT"为标记的tag相关信息
              if (usefulTagtypeS.contains(kwdtype(1))) {
                if (!kwds.contains(kwd))
                  kwds.append(kwd)
              }
            }
          }
          if (kwds.size > 0) {
            val idtype = videoId + "-" + vType
            videoKwdM.put(idtype, kwds)
          }

        }
      }catch {
        case ex: Exception => println("io exception" + ex + ", line=" + line)
      }
      line = reader.readLine()
    }
    reader.close()
  }

  def getSingleSize(sep: String) = udf((str:String) => {
    str.split(sep).toSeq.filter(info => !info.contains("&")).size
  })

  def getMultiSize(sep: String) = udf((str:String) => {
    str.split(sep).toSeq.filter(info => info.contains("&")).size
  })
}
