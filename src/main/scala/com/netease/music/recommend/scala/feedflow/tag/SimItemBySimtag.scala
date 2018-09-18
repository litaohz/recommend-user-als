package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._
import java.util
import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.util.Random


/**
  * 根据用户偏好tag + 相似tag + tag-video 推荐video
  * Created by hzlvqiang on 2017/12/4.
  */
object SimItemBySimtag {

  def isDigit(tag: String) = {
    val regex="""^\d+$""".r
    if (regex.findFirstMatchIn(tag) == None) {
      false
    } else {
      true
    }
  }

  def splitKV(tagScoreStr: String, str: String) = {
    val idx = tagScoreStr.lastIndexOf(str)
    (tagScoreStr.substring(0, idx), tagScoreStr.substring(idx+1))
  }

  def getSimsMFile(reader: BufferedReader, tagVideosM: mutable.HashMap[String, Array[String]], tagSimtagM: mutable.HashMap[String, ArrayBuffer[Tuple2[String, Float]]]) = {
    var string: String = null
    try {
      while ((string = reader.readLine()) != null) {
        // tag \t tag:score,tag:score,...
        val ts = string.split("\t", 2)
        val tag = ts(0)
        if (tagVideosM.contains(tag)) {
          val tagScores = mutable.ArrayBuffer[Tuple2[String, Float]]()
          breakable {
            for (tagScoreStr <- ts(1).split(",")) {
              val tagScore = splitKV(tagScoreStr, ":")
              val stag = tagScore._1
              if (tagVideosM.contains(stag) && !tagScore._2.equals("Infinity")) {
                try {
                  val score = tagScore._2.toFloat
                  tagScores.append((stag, score))
                  if (tagScores.size > 40) {
                    break()
                  }
                } catch {
                  case ext: Exception => println("format exceptions:" + string + " => " + tagScoreStr + "=>" + tagScore._2)
                    throw new RuntimeException()
                }

              }
            }
          }
          tagSimtagM.put(tag, tagScores)
          if (tagSimtagM.size % 10000 == 0) {
            println("load tag=" + tag)
          }
        }
      }
    }catch {
      case ex: Exception => println("io excpeiton" + ex)
    }

  }

  /*
        return {tag ->  [simtag:score, simtag:score] }
         */
  def getSimsM(inputDir: String , tagVideosM: mutable.HashMap[String, Array[String]] ) = {
    val tagSimtagM = new mutable.HashMap[String, ArrayBuffer[Tuple2[String, Float]]]()

    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        getSimsMFile(bufferedReader, tagVideosM, tagSimtagM)
        bufferedReader.close()
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSimsMFile(bufferedReader, tagVideosM, tagSimtagM)
    }
    tagSimtagM

  }

  def getTagScoreM(tss: Array[String]) = {
    val notPrefTagScoreM = mutable.LinkedHashMap[String, Double]()
    for (i <- 0 until tss.length/5) { // 20%低偏好的tag去重
      val ts = tss.apply(i).split(":")
      notPrefTagScoreM.put(ts(0), ts(1).toDouble)
    }
    val prefTagScoreM = mutable.LinkedHashMap[String, Double]()
    for (i <- tss.length/2 until tss.length) { // 30%高偏好进行关联，并去重
      val ts = tss.apply(i).split(":")
      val wt = ts(1).toDouble
      if (wt > 0) {
        prefTagScoreM.put(ts(0), wt)
      }
    }
    (notPrefTagScoreM, prefTagScoreM)
  }

  def getVideoTagsM(strings: Array[String], videoScoreM : mutable.HashMap[String, Double]) = {
    val tagVideosM = mutable.HashMap[String, mutable.HashSet[String]]()
    // vid \t type \t tag_TYPE,tag_TYPE
    for (string <- strings) {
      val ts = string.split("\t")
      val tags = ts(2).split(",")
      if (ts(1).equals("video")) {
        // val idtype = ts(0) + ":" + ts(1)
        for (tag <- tags) {
          val wtype = tag.split("_")
          if (!wtype(1).equals("CC") && !wtype(1).equals("WD") && !wtype(1).equals("OT") && !wtype(1).equals("KW")) {
            val tag = wtype(0)
            if (!tagVideosM.contains(tag)) {
              tagVideosM.put(tag, mutable.HashSet[String]())
            }
            var videos = tagVideosM.apply(tag)
            videos.add(ts(0))
          }
        }
      }
    }
    // 按照质量排序
    val tagVideosResM = mutable.HashMap[String, Array[String]]()
    for ((tag, videos) <- tagVideosM) {
      if (videos.size >= 5 && videos.size <= 4000) { // 忽略掉一些低频和高频干扰
        val sortedVideos = videos.toArray[String].sortWith { (v1, v2) =>
          val v1Score = videoScoreM.getOrElse(v1, 0.0)
          val v2Score = videoScoreM.getOrElse(v2, 0.0)
          v1Score > v2Score
        }
        tagVideosResM.put(tag, sortedVideos)
      }
    }
    (null, tagVideosResM)
  }

  def getVideoPredM(rows: Array[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      if (row.getAs[String]("vType").equals("video")) {
        val vid = row.getAs[Long]("videoId").toString
        val rawPred = row.getAs[Double]("rawPrediction")
        idtypePredM.put(vid, rawPred)
      }
    }
    idtypePredM
  }

  def getVideoPredM(viter: util.Iterator[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    while (viter.hasNext) {
      val row = viter.next()
      if (row.getAs[String]("vType").equals("video")) {
        val vid = row.getAs[String]("videoId")
        val rawPred = row.getAs[Double]("rawPrediction")
        idtypePredM.put(vid, rawPred)
      }
    }
    idtypePredM
  }

  case class UidPreftag(userId: Long, ttype: String, prefTags: String)
  case class UidRcedVideos(userId: Long, rcedVideos: String)
  case class UidRcedVideosFromEvent(userId: Long, rcedVideosFromEvent: String)

  def getSet(str: String, videos: mutable.HashSet[String]) = {
    if (str != null) {
      for (video <- str.split(",")) {
        videos.add(video)
      }
    }
  }

  def vidReasonStr(videoReasonM: mutable.HashMap[String, String]): String = {
    var res = ""
    if (videoReasonM != null && !videoReasonM.isEmpty) {
      val sbuff = mutable.ArrayBuffer[String]()
      for ((vid, reason) <- videoReasonM) {
        // sbuff.append(vid + ":video:" + reason +"-tag")
        sbuff.append(vid + ":video") // 先忽略掉理由，下游排序不支持
      }
      res = sbuff.mkString(",")
    }
    res
  }


  def getTopTagVideos(tagVideosMLocal: mutable.HashMap[String, Array[String]], sortedCandTag: Seq[(String, Double)], rcedVideos: mutable.HashSet[String], num: Int) = {
    val videoReasonM = mutable.HashMap[String, String]()
    breakable {
      for ((candTag, score) <- sortedCandTag) {
        val videos = tagVideosMLocal.getOrElse(candTag, null)
        if (videos != null) {
          breakable {
            for (vid <- videos) {
              if (!rcedVideos.contains(vid)) {
                videoReasonM.put(vid, candTag)
                break()
              }
            }
          }
        }
        if (videoReasonM.size > num) {
          break()
        }
      }
    }
    videoReasonM
  }

  def getVideosRandomByChance(tagVideosMLocal: mutable.HashMap[String, Array[String]], sortedCandTag: Seq[(String, Double)], rcedVideos: mutable.HashSet[String], num: Int) = {
    // val videoReasonM = mutable.HashMap[String, String]()
    val videoReasonList = mutable.ArrayBuffer[(String, String)]()
    val random = new Random()
    var cnt = 0

    for ((candTag, score) <- sortedCandTag) { // 分数越大的越排后面
      cnt += 1
      if (videoReasonList.length >= num) {
        var rmIdx = random.nextInt(cnt + 1)
        if (rmIdx < videoReasonList.size || random.nextInt(sortedCandTag.length * 2) < cnt) { // 平均概率 || 越排后面，被选中概率越大
          if (rmIdx >= videoReasonList.size) {
            rmIdx = random.nextInt(cnt + 1)
            if (rmIdx < videoReasonList.size) {
              videoReasonList.remove(rmIdx)
            }
          }
        }
      }
      if (videoReasonList.size < num) {
        val videos = tagVideosMLocal.getOrElse(candTag, null)
        if (videos != null) {
          breakable {
            for (vid <- videos) {
              if (!rcedVideos.contains(vid)) {
                videoReasonList.append((vid, candTag))
                // videoReasonM.put(vid, candTag)
                break()
              }
            }
          }
        }
      }
    }
    val videoReasonM = mutable.HashMap[String, String]()
    for (vidtag <- videoReasonList) {
      videoReasonM.put(vidtag._1, vidtag._2)
    }
    videoReasonM
  }

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf();
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val options = new Options()
    options.addOption("user_pref_tag", true, "user_pref_tag")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("user_rced_video_from_event", true, "user_rced_video_from_event")
    options.addOption("simtag_w2v", true, "simtag_w2v")
    options.addOption("simtag_als", true, "simtag_als")
    options.addOption("simtag_dnlp", true, "simtag_dnlp")
    options.addOption("video_feature", true, "video_feature")
    options.addOption("video_pred_output", true, "video_pred_output")
    options.addOption("tag_video_output", true, "tag_video_output")

    options.addOption("video_feature_warhouse", true, "video_feature_warhouse")
    options.addOption("video_tags", true, "video_tags")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    println("读取视频特征数据...")
    val inputVideoFeatrue = cmd.getOptionValue("video_feature")
    println("inputVideoFeatrue:" + inputVideoFeatrue)
    val viter = spark.read.parquet(inputVideoFeatrue).toDF().select("videoId", "vType", "rawPrediction").collect()
    // println("vparray size:" + vparray.)
    val videoPredM = getVideoPredM(viter)
    //val videoPredM = getVideoPredM(viter)
    println("vieoPrefM size:" + videoPredM.size)

    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(cmd.getOptionValue("video_pred_output"))
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((key, value) <- videoPredM) {
      bwriter.write(key + "\t" + value + "\n")
    }
    bwriter.close()

    val inputVidTags = cmd.getOptionValue("video_tags")
    val tagVideosM = getVideoTagsM(spark.read.textFile(inputVidTags).collect(), videoPredM)._2
    outpath = new Path(cmd.getOptionValue("tag_video_output"))
    bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((tag, videos) <- tagVideosM) {
      bwriter.write(tag + "\t" + videos.mkString(",") + "\n")
    }
    bwriter.close()
    println("tagVideosM size:" + tagVideosM.size)
    println("海贼王 videos->" + tagVideosM.apply("海贼王"))
    val tagVideosMBroad = sc.broadcast(tagVideosM)

    println("读取相关tag数据als...")
    val inputAls = cmd.getOptionValue("simtag_als")
    val rdd = sc.textFile(inputAls)


    // var inputStream: FSDataInputStream = null
    // var bufferedReader: BufferedReader = null
    //val hdfs: FileSystem = FileSystem.get(new Configuration())
    //转成缓冲流
    //var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputAls))))

    val alsTagSimtagScoreM = getSimsM(inputAls, tagVideosM)
    println("alsTagSimtagScoreM size:" + alsTagSimtagScoreM.size)
    val alsTagSimtagScoreMBroad = sc.broadcast(alsTagSimtagScoreM)

    println("读取相关tag数据w2v...")
    val inputW2V = cmd.getOptionValue("simtag_w2v")
    // val w2vTagSimtagScoreM = getSimsM(spark.read.textFile(inputW2V).toLocalIterator(), tagVideosM)
    // val w2vTagSimtagScoreM = getSimsM(sc.textFile(inputW2V), tagVideosM)
    //bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputW2V))))
    // val w2vTagSimtagScoreM = getSimsM(bufferedReader, tagVideosM)
    val w2vTagSimtagScoreM = getSimsM(inputW2V, tagVideosM)
    println("w2vTagSimtagScoreM size:" + w2vTagSimtagScoreM.size)
    val w2vTagSimtagScoreMBroad = sc.broadcast(w2vTagSimtagScoreM)
    println("读取相关tag数据nlp...")
    val inputNLP = cmd.getOptionValue("simtag_dnlp")
    // val nlpTagSimtagScoreM = getSimsM(spark.read.textFile(inputNLP).toLocalIterator(), tagVideosM)
    //bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputNLP))))
    // val nlpTagSimtagScoreM = getSimsM(sc.textFile(inputNLP), tagVideosM)
    val nlpTagSimtagScoreM = getSimsM(inputNLP, tagVideosM)
    println("nlpTagSimtagScoreM size:" + nlpTagSimtagScoreM.size)
    val nlpTagSimtagScoreMBroad = sc.broadcast(nlpTagSimtagScoreM)

    println("读取用户偏好数据...")
    val inputUserPrefTag = cmd.getOptionValue("user_pref_tag")
    val userPreftagData = spark.read.textFile(inputUserPrefTag).map(line => {
      // uid \t tag:score, tag:score  // score从小到大排列
      val ts = line.split("\t", 3)
      UidPreftag(ts(0).toLong, ts(1), ts(2))
    }).toDF("userId", "ttype", "prefTags").filter($"ttype" === "T4")

    println("userPreftagData cnt:" + userPreftagData.count())

    println("读取用户已经推荐数据1...")
    val inputUserRced = cmd.getOptionValue("user_rced_video")
    val userRcedVideosData = spark.read.textFile(inputUserRced).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      UidRcedVideos(ts(0).toLong, ts(1))
    }).toDF("userId", "rcedVideos")
    println("userRcedVideosData cnt:" + userRcedVideosData.count())

    println("读取用户已经推荐数据2...")
    val inputUserRcedFromEvent = cmd.getOptionValue("user_rced_video_from_event")
    val userRcedVideosFromEventData = spark.read.textFile(inputUserRcedFromEvent).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      UidRcedVideosFromEvent(ts(0).toLong, ts(1))
    }).toDF("userId", "rcedVideosFromEvent")
    println("userRcedVideosFromEventData cnt:" + userRcedVideosFromEventData.count())

    val rcmdByVec = userPreftagData.join(userRcedVideosData, Seq("userId"), "left").join(userRcedVideosFromEventData, Seq("userId"), "left")
        .flatMap(row => {
          val flatOut = mutable.ArrayBuffer[Tuple2[Integer, String]]()
          try {
            val alsTagSimtagScoreMLocal: mutable.HashMap[String, ArrayBuffer[Tuple2[String, Float]]] = alsTagSimtagScoreMBroad.value
            val w2vTagSimtagScoreMLocal: mutable.HashMap[String, ArrayBuffer[Tuple2[String, Float]]] = w2vTagSimtagScoreMBroad.value
            val nlpTagSimtagScoreMLocal: mutable.HashMap[String, ArrayBuffer[Tuple2[String, Float]]] = nlpTagSimtagScoreMBroad.value

            val userId = row.getAs[Long]("userId")
            val prefTags = row.getAs[String]("prefTags").split(",")
            val rcedVideos = mutable.HashSet[String]() // 已推荐过视频
            getSet(row.getAs[String]("rcedVideos"), rcedVideos)
            getSet(row.getAs[String]("rcedVideosFromEvent"), rcedVideos)

            val tagPair = getTagScoreM(prefTags)
            // 不喜欢的buttom
            val notPrefTagScoreM = tagPair._1
            // 喜欢的top
            val prefTagScoreM = tagPair._2
            val candTagScoreMals = mutable.HashMap[String, Double]()
            val candTagScoreMw2v = mutable.HashMap[String, Double]()
            val candTagScoreMnlp = mutable.HashMap[String, Double]()
            for ((tag, score) <- prefTagScoreM) {
              val simtagScoresAls = alsTagSimtagScoreMLocal.getOrElse(tag, null)
              if (simtagScoresAls != null) {
                for ((simtag, simscore) <- simtagScoresAls) {
                  var lstScore: Double = candTagScoreMals.getOrElse(simtag, 0)
                  // TODO 不要过滤
                  if (!prefTagScoreM.contains(simtag) && !notPrefTagScoreM.contains(simtag)) {
                    candTagScoreMals.put(simtag, lstScore + (simscore * score))
                  }
                }
              }
              val simtagScoresW2v = w2vTagSimtagScoreMLocal.getOrElse(tag, null)
              if (simtagScoresW2v != null) {
                for ((simtag, simscore) <- simtagScoresW2v) {
                  var lstScore: Double = candTagScoreMw2v.getOrElse(simtag, 0)
                  // TODO 不要过滤
                  if (!prefTagScoreM.contains(simtag) && !notPrefTagScoreM.contains(simtag)) {
                    candTagScoreMw2v.put(simtag, lstScore + (simscore * score))
                  }
                }
              }
              val simtagScoresNlp = nlpTagSimtagScoreMLocal.getOrElse(tag, null)
              if (simtagScoresNlp != null) {
                for ((simtag, simscore) <- simtagScoresNlp) {
                  var lstScore1: Double = candTagScoreMnlp.getOrElse(simtag, 0)
                  // TODO 不要过滤
                  if (!prefTagScoreM.contains(simtag) && !notPrefTagScoreM.contains(simtag)) {
                    candTagScoreMnlp.put(simtag, lstScore1 + (simscore * score))
                  }
                }
              }
            }

            // TODO 对权重少的降权，不完全取新视频
            var sortedCandTag = candTagScoreMals.toSeq.sortBy[Double](-_._2) // 倒序排列
            if (userId == 359792224) {
              println("rced num of 3597 als:" + rcedVideos.size)
              println("rank cands...")
              println(sortedCandTag)
            }
            // 每个tag取predict最高的一个video
            // val videoReasonM = mutable.HashMap[String, String]()
            val tagVideosMLocal = tagVideosMBroad.value
            var videoReasonM = getTopTagVideos(tagVideosMLocal, sortedCandTag, rcedVideos, 20)
            //userId + "\t" + vidReasonStr(videoReasonM)
            flatOut.append((1, userId + "\t" + vidReasonStr(videoReasonM)))


            sortedCandTag = candTagScoreMw2v.toSeq.sortBy[Double](-_._2) // 倒序排列
            if (userId == 359792224) {
              println("rced num of 3597 w2v:" + rcedVideos.size)
              println("rank cands...")
              println(sortedCandTag)
            }
            // 每个tag取predict最高的一个video
            videoReasonM = getTopTagVideos(tagVideosMLocal, sortedCandTag, rcedVideos, 20)
            // userId + "\t" + vidReasonStr(videoReasonM)
            flatOut.append((2, userId + "\t" + vidReasonStr(videoReasonM)))

            sortedCandTag = candTagScoreMnlp.toSeq.sortBy[Double](-_._2) // 倒序排列
            if (userId == 359792224) {
              println("rced num of 3597 w2v:" + rcedVideos.size)
              println("rank cands...")
              println(sortedCandTag)
            }
            // 每个tag取predict最高的一个video
            videoReasonM = getTopTagVideos(tagVideosMLocal, sortedCandTag, rcedVideos, 20)
            // userId + "\t" + vidReasonStr(videoReasonM)
            flatOut.append((3, userId + "\t" + vidReasonStr(videoReasonM)))

            // 本身输出
            videoReasonM = getVideosRandomByChance(tagVideosMLocal, prefTagScoreM.toSeq, rcedVideos, 20)
            flatOut.append((4, userId + "\t" + vidReasonStr(videoReasonM)))
            if (userId == 359792224) {
              println("rced num of 3597 tagArt:" + rcedVideos.size)
              println("tag cand:" + prefTagScoreM.size)
              for (vr <- videoReasonM) {
                println("rank cands...")
                println(vr._1 + "->" + vr._2)
              }
            }

          } catch {
            case exp:Exception => println("inline excpeption:" + exp)
          }

          flatOut
    }).toDF("st", "data").filter(!$"data".endsWith("\t"))
    println("sample Data...")
    rcmdByVec.show(10)

    rcmdByVec.filter($"st" === 1).select("data").write.text(cmd.getOptionValue("output") + "/simAls")
    rcmdByVec.filter($"st" === 2).select("data").write.text(cmd.getOptionValue("output") + "/simW2v")
    rcmdByVec.filter($"st" === 3).select("data").write.text(cmd.getOptionValue("output") + "/simNlp")
    rcmdByVec.filter($"st" === 4).select("data").write.text(cmd.getOptionValue("output") + "/tagArt")
    // rcmdByVec.write.text(cmd.getOptionValue("output"))

  }
}
