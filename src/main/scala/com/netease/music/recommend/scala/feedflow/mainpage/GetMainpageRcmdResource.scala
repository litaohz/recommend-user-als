package com.netease.music.recommend.scala.feedflow.mainpage

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.util.Date

import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 从排序视频中获取分类
  * Created by hzzhangjunfei on 2018/02/14.
  */
object GetMainpageRcmdResource {

  val CAND_MIN_NUM_FOR_SINGLE_KEY = 4
  val CAND_MAX_NUM_FOR_SINGLE_KEY = 6

  val CAND_MAX_NUM_FOR_SINGLE_RESOURCE = 10
  val CAND_MAX_NUM_FOR_GUESS = 50

  val KWD_RESOURCE_THRED = 2
  val GUESS_RESOURCE_THRED = 1

  val RESOURCE_PER_KWD = 2
  val VIDEO_PER_KWD = 3

  val NOW_TIME = System.currentTimeMillis()

  val KWD_REC_WINDOW = 7
  val SONGART_REC_WINDOW = 7

  val usefulTagtypeS = Set[String]("ET", "AT", "WK", "PT", "TT")
  // alg
  val usefulResourcetypeS = Set[String]("mv", "evtTopic", "concert")
  // feature
  val usefulCombotypeS = Set[String]("keyword_f", "comboConcert_f", "newMv_f")
  val usefulSingletypeS = Set[String]("mv_f", "video_f", "concert_f", "newConcert_f", "evtTopic_f")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options

    options.addOption("Music_MVMeta_allfield", true, "mv_input")
    options.addOption("Music_VideoRcmdMeta", true, "video_input")
    options.addOption("mvPool", true, "mv pool input")
    options.addOption("Music_Song_artists", true, "music_song_input")
    options.addOption("Music_RcmdFeedBlock", true, "music_feature_input")
    options.addOption("videoPool", true, "videoPool")
    options.addOption("Music_VideoTag", true, "Music_VideoTag")
    options.addOption("Music_RcmdResource", true, "Music_RcmdResource")
    options.addOption("video_tags", true, "video_tags")
    options.addOption("Music_UserProfile2", true, "Music_UserProfile2")

    options.addOption("mainpage_keywords", true, "mainpage_keywords")
    options.addOption("video_prd", true, "video_prd")

    options.addOption("algMerge", true, "algMerge")
    options.addOption("songPref", true, "songPref")
    options.addOption("reced_videos", true, "reced_videos")
    options.addOption("reced_keyword", true, "reced_keyword")

    options.addOption("hot_videos", true, "hot_videos")
    options.addOption("video_dup", true, "video_dup")
    options.addOption("stopwords", true, "stopwords")

    options.addOption("art_tags", true, "art_tags")
    options.addOption("art_sims", true, "art_sims")
    options.addOption("user_sub_art", true, "user_sub_art")
    //options.addOption("musicEventCategories", true, "input directory")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    var kwdTagM = Map[String, String]()
    scala.io.Source.fromInputStream(getClass.getResourceAsStream("/tagKwdMappings"))
      .getLines
      .foreach{line =>
        val info = line.split("\t")
        val kwd = info(0)
        val tag = info(1)
        kwdTagM += (kwd -> tag)
      }
    val kwdTagMBrod = sc.broadcast(kwdTagM)
    println("kwdTagM size:" + kwdTagM.size + ",\tfirst item:" + kwdTagM.iterator.next.toString)

    import spark.implicits._

    val vtagIdNameM = loadVideoTagIdNameM(cmd.getOptionValue("Music_VideoTag"))
    println("vtagIdNameM size:" + vtagIdNameM.size + ",\tfirst item:" + (if (vtagIdNameM.size>0) vtagIdNameM.iterator.next else "null"))
    val provincenameCodeM = getProvincenameCodeM
    val sidResourcesM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val aidsidResourcesM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val aidResourcesM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val kwdResourceM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val resourceKwdM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    //    val tagResourceM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    //    val resourceTagM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val ridSidAidM = mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]()
    val ridPcodeM = mutable.HashMap[String ,String]()
    val newconcertS = mutable.Set[String]()
    // 获取运营concert、keyword
    val featureInput = cmd.getOptionValue("Music_RcmdFeedBlock")
    println("Music_RcmdFeedBlockInput:" + featureInput)
    //    val feature_ridTitleM = getFeatureResourceInfoM(featureInput, vtagIdNameM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidM, aidsidResourcesM, provincenameCodeM, ridPcodeM)
    val feature_ridTitleM = mutable.HashMap[String, String]()
    spark.read.json(featureInput)
      .collect()
      .map { line =>
        /*val status = line.getAs[Int]("status")
        val forceRcmd = line.getAs[Int]("forceRcmd")
        val onlineTime = line.getAs[Long]("onlineTime")
        val offlineTime = line.getAs[Long]("offlineTime")*/
        val blocktype = parseBlocktype2resourcetype(line.getAs[Long]("blockType"))
        var resourcetype = ""
        var content = ""
        implicit val formats = DefaultFormats
        if (blocktype.equals("single")) {
          content = line.getAs[String]("content")
          if (content != null) {
            val contentJson = parse(content)
            resourcetype = parse2type_feature((contentJson \ "resourceType").extractOrElse[Int](-1).toString)
          }
        } else {
          resourcetype = blocktype
        }
        val rid = line.getAs[Long]("id")
        if (
        //          status == 1 && forceRcmd == 0 && offlineTime >= NOW_TIME && onlineTime <= NOW_TIME &&
          (usefulCombotypeS(resourcetype) || usefulSingletypeS.contains(resourcetype))
        ) {
          val songIds = parseIds(line.getAs[String]("songIds"))
          val artistIds = parseIds(line.getAs[String]("artistIds"))
          val auditTagIds = parseIds(line.getAs[String]("auditTagIds"))
          var title = line.getAs[String]("title")
          title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
          val regions = line.getAs[String]("regions")
          var provinceCode = "-1"
          if (regions != null && !regions.isEmpty) {
            val provinceName = (parse(regions) \ "province").extractOrElse[String]("null")
            provinceCode = parse2ProvinceCode(provinceName, provincenameCodeM)
          }
          //println(s"feature data=blocktype:${blocktype},resourcetype:${resourcetype},rid:${rid},songIds:${songIds},artistIds:${artistIds},auditTagIds:${auditTagIds},title:${title},regions:${regions},provinceCode:${provinceCode}")

          if (!resourcetype.isEmpty) {
            val ridType = rid + "-" + resourcetype
            feature_ridTitleM.put(ridType, title)
            if (!provinceCode.equals("-1"))
              ridPcodeM.put(ridType, provinceCode)

            if (songIds != null) {
              for (sid <- songIds) {
                if (!sid.isEmpty) {
                  val resources = sidResourcesM.getOrElse(sid, mutable.ArrayBuffer[String]())
                  sidResourcesM.put(sid, resources)
                  resources.append(ridType)

                  val sidAidPair = ridSidAidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidM.put(ridType, sidAidPair)
                  sidAidPair._1.append(sid)
                }
              }
            }

            if (artistIds != null) {
              for (aid <- artistIds) {
                if (!aid.isEmpty) {
                  val resources = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
                  aidResourcesM.put(aid, resources)
                  resources.append(ridType)

                  val sidAidPair = ridSidAidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidM.put(ridType, sidAidPair)
                  sidAidPair._2.append(aid)
                }
              }
            }

            if (auditTagIds != null) {
              for (tid <- auditTagIds) {
                if (!tid.isEmpty) {
                  val kwd = vtagIdNameM.getOrElse[String](tid, "")
                  if (!kwd.isEmpty) {

                    val resources = kwdResourceM.getOrElse(kwd, mutable.ArrayBuffer[String]())
                    kwdResourceM.put(kwd, resources)
                    resources.append(ridType)

                    val kwds = resourceKwdM.getOrElse(ridType, mutable.ArrayBuffer[String]())
                    resourceKwdM.put(ridType, kwds)
                    kwds.append(kwd)
                  }
                }
              }
            }

            if (songIds != null && artistIds != null) {
              for (sid <- songIds; aid <- artistIds) {
                val aidsid = aid + ":" + sid
                val ridTypes = aidsidResourcesM.getOrElse(aidsid, ArrayBuffer[String]())
                aidsidResourcesM.put(aidsid, ridTypes)
                if (!ridTypes.contains(ridType))
                  ridTypes.append(ridType)
              }
            }
          }
        } /*else {
          println(s"feature data filtered : blocktype:${blocktype},resourcetype:${resourcetype},rid:${rid}")
        }*/
      }

    // 获取concert、keyword
    val resourceInput = cmd.getOptionValue("Music_RcmdResource")
    println("Music_RcmdResourceInput:" + resourceInput)
    val ridTitleM = getResourceInfoM(resourceInput, vtagIdNameM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidM, aidsidResourcesM, provincenameCodeM, ridPcodeM, newconcertS)
    val kwdResourceMBroad = sc.broadcast(kwdResourceM)
    val resourceKwdMBroad = sc.broadcast(resourceKwdM)
    //    val tagResourceMBroad = sc.broadcast(tagResourceM)
    //    val resourceTagMBroad = sc.broadcast(resourceTagM)

    val artTagsInput = cmd.getOptionValue("art_tags")
    // 获取aid及其对应的tags
    val artTagsM = getArtTagsM(artTagsInput)
    println("artTagsM size:" + artTagsM.size)
    // 将aid及其对应的tag输出
    sc.parallelize[String](artTagsM.map(tup => tup._1 + "\t" + tup._2(0)).toSeq)
      .saveAsTextFile(cmd.getOptionValue("output") + "/artist_tag")
    val artTagsMBroad = sc.broadcast(artTagsM)
    // 获取aid及其对应的simArtist
    val artSimsInput = cmd.getOptionValue("art_sims")
    val artSimsM = mutable.HashMap[String, mutable.LinkedHashSet[String]]()
    getArtSimsM(new Path(artSimsInput), artSimsM)
    println("artSimsM size:" + artSimsM.size)
    val artSimsMBroad = sc.broadcast(artSimsM)

    // 获取vid及其对应的rawPrediction
    val videoPredInput = cmd.getOptionValue("video_prd")
    println("Load video_prd:" + videoPredInput)
    val videoPredM = getVideoPredM(videoPredInput)
    println("videoPredM size:" + videoPredM.size)
    println("video=213295, prd=" + videoPredM.get("213295"))

    // 获取vid及其对应的maxRawPrediction vid
    val vid2TopvidM = getVideoDupM(cmd.getOptionValue("video_dup"), videoPredM) // 重复视频


    val videoPoolInput = cmd.getOptionValue("videoPool")
    println("videoPoolInput:" + videoPoolInput)
    val musicVideoS = spark.read.parquet(videoPoolInput)
      .filter($"isMusicVideo"===1 && $"vType"==="video")
      //.filter($"groupIdAndNames".contains("12102:只推首页") && $"vType"==="video")
      .select("videoId")
      .map(row => row.getLong(0).toString)
      .collect()
      .toSet
    //      .filter(vid => vidTitleMBroad.value.contains(vid))
    val musicVideoSBroad = sc.broadcast(musicVideoS)

    val videoInput = cmd.getOptionValue("Music_VideoRcmdMeta")
    println("videoMetaInput:" + videoInput)
    val sidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]]()
    val aidsidVideosM = mutable.HashMap[String, String]()
    val aidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]]()
    val vidSidAidM = mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]()
    val vidTitleM = getVideoInfoM(videoInput, sidVideosM, aidVideosM, videoPredM, vidSidAidM, aidsidVideosM, musicVideoS)
    println("vidTitleM size before clear:" + vidTitleM.size)

    val stopwords = getStopwords(cmd.getOptionValue("stopwords"))
    removeInvalid(vidTitleM, vid2TopvidM, stopwords, videoPredM)

    println("sidVideosM size:" + sidVideosM.size)
    println("aidVideosM size:" + aidVideosM.size)
    println("vidTitleM size:" + vidTitleM.size)
    /*println("videos for songid=186016")
    for ((vid, prd) <- sidVideosM.getOrElse("186016", null)) {
      println("vid=" + vid + ", prd=" + prd)
    }
    println("vid=213295, title=" + vidTitleM.get("213295"))
    println("aid=6452(周杰伦), videos size:" + aidVideosM.get("6452").size + ", firt=" + aidVideosM.getOrElse("6452", mutable.ArrayBuffer[(String, Float)]())(1))*/
    // 输出 vid  \t  title
    flushVideoTitle(vidTitleM, cmd.getOptionValue("output") + "/vid_title")

    // aid:sid    vid
    val aidsidVideosMBroad = sc.broadcast(aidsidVideosM)
    // aid    (vid,rawPrediction),...
    val aidVideosMBroad = sc.broadcast(aidVideosM)
    // sid    (vid,rawPrediction),...
    val vidSidAidMBroad = sc.broadcast(vidSidAidM)
    // vid    title
    val vidTitleMBroad = sc.broadcast(vidTitleM)



    /* 先暂时使用运营池中的热门视频
    println("过滤热门视频")
    val hotVideos = spark.read.text(cmd.getOptionValue("hot_videos")).collect()
    // 将热门视频输出
    flushHotVideo(vidTitleM, hotVideos, cmd.getOptionValue("output") + "/hotvideo")*/

    println("读取MV pool...")
    val mvPoolInput = cmd.getOptionValue("mvPool")
    val newmvS = mutable.Set[String]()
    val mvPoolS = mutable.Set[String]()
    println("mvPool:" + mvPoolInput)
    spark.read.parquet(mvPoolInput)
      .collect()
      .map{line =>
        val mvId = line.getAs[Long]("mvId").toString
        val artistIds = line.getAs[String]("artistIds")
        val songIds = line.getAs[String]("songIds")
        val isNewMv = line.getAs[Boolean]("isNewMv")
        val rawPrediction = line.getAs[Double]("rawPrediction")

        if (isNewMv)
          newmvS += mvId
        mvPoolS += mvId

        val mvidtype = mvId + "-mv"
        val aids = mutable.ArrayBuffer[String]()
        val sids = mutable.ArrayBuffer[String]()

        if (artistIds != null && !artistIds.equals("null") && !artistIds.equals("0")) {
          for (aid <- artistIds.split("_tab_")) {
            aids.append(aid)

            val ridtypes = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
            if (!ridtypes.contains(mvidtype)) {
              aidResourcesM.put(aid, ridtypes)
              ridtypes.append(mvidtype)
            }

            val sidAidPair = ridSidAidM.getOrElse(mvidtype, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
            ridSidAidM.put(mvidtype, sidAidPair)
            if (!sidAidPair._2.contains(aid))
              sidAidPair._2.append(aid)
          }
        }

        if (songIds != null && !songIds.equals("null") && !songIds.equals("0")) {
          for (sid <- songIds.split("_tab_")) {
            sids.append(sid)

            val ridtypes = sidResourcesM.getOrElse(sid, mutable.ArrayBuffer[String]())
            if (!ridtypes.contains(mvidtype)) {
              sidResourcesM.put(sid, ridtypes)
              ridtypes.append(mvidtype)
            }

            val sidAidPair = ridSidAidM.getOrElse(mvidtype, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
            ridSidAidM.put(mvidtype, sidAidPair)
            if (!sidAidPair._1.contains(sid))
              sidAidPair._1.append(sid)
          }
        }

        if (!sids.isEmpty && !aids.isEmpty) {
          for (sid <- sids; aid <- aids) {
            val aidsid = aid + ":" + sid
            val ridTypes = aidsidResourcesM.getOrElse(aidsid, mutable.ArrayBuffer[String]())
            aidsidResourcesM.put(aidsid, ridTypes)
            if (!ridTypes.contains(mvidtype))
              ridTypes.append(mvidtype)
          }
        }
      }

    println("读取MV META...")
    val mvInput = cmd.getOptionValue("Music_MVMeta_allfield")
    println("mvInput:" + mvInput)
    val aidTags = getinfoRromMV(mvInput, mvPoolS, ridSidAidM, aidResourcesM, sidResourcesM, aidsidResourcesM)

    // aid=8325(梁静茹), tags=Map(港台女歌手 -> 5, 港台流行男歌手 -> 1, 港台乐队/组合 -> 1, 港台流行女歌手 -> 10, 马来西亚流行女歌手 -> 5)
    // aid=6452(周杰伦), tags=Map(港台摇滚男歌手 -> 1, 港台流行乐队/组合 -> 5, 内地乐队/组合 -> 1, 港台原声男歌手 -> 1, 内地流行女歌手 -> 2, 欧美流行乐队/组合 -> 2, 港台流行男歌手 -> 257, 欧美流行男歌手 -> 1, 港台乐队/组合 -> 2, 大陆流行女歌手 -> 2, 大陆乐队/组合 -       > 1, 大陆流行乐队/组合 -> 3, 港台R&B男歌手 -> 9, 港台男歌手 -> 46, 港台流行 -> 1, 内地流行乐队/组合 -> 3)
    println("aid=8325(梁静茹), tags=" + aidTags.getOrElse("8325", "null"))
    println("ridSidAidM size:" + ridSidAidM.size)
    println("aidTags size:" + aidTags.size)
    println("aid=6452(周杰伦), tags=" + aidTags.getOrElse("6452", "null"))
    println("aidResourcesM size:" + aidResourcesM.size)
    println("aid=6452(周杰伦), ridTypes=" + aidResourcesM.getOrElse("6452", "null"))
    println("aid=1007170(陈粒), ridTypes=" + aidResourcesM.getOrElse("1007170", "null"))
    println("sidResourcesM size:" + sidResourcesM.size)
    println("aidsidResourcesM size:" + aidsidResourcesM.size)

    println("feature_ridTitleM size:" + feature_ridTitleM.size + ",\tfirst item:" + (if (feature_ridTitleM.size>0) feature_ridTitleM.iterator.next else "null"))
    println("ridTitleM size:" + ridTitleM.size + ",\tfirst item:" + (if (ridTitleM.size>0) ridTitleM.iterator.next else "null"))
    println("sidResourcesM size:" + sidResourcesM.size + ",\tfirst item:" + (if (sidResourcesM.size>0) sidResourcesM.iterator.next else "null"))
    println("aidResourcesM size:" + aidResourcesM.size + ",\tfirst item:" + (if (aidResourcesM.size>0) aidResourcesM.iterator.next else "null"))
    println("aid=6452(周杰伦), ridTypes=" + aidResourcesM.getOrElse("6452", "null"))
    println("kwdResourceM size:" + kwdResourceM.size + ",\tfirst item:" + (if (kwdResourceM.size>0) kwdResourceM.iterator.next else "null"))
    println("resourceKwdM size:" + resourceKwdM.size + ",\tfirst item:" + (if (resourceKwdM.size>0) resourceKwdM.iterator.next else "null"))
    //    println("tagResourceM size:" + tagResourceM.size + ",\tfirst item:" + (if (tagResourceM.size>0) tagResourceM.iterator.next else "null"))
    //    println("resourceTagM size:" + resourceTagM.size + ",\tfirst item:" + (if (resourceTagM.size>0) resourceTagM.iterator.next else "null"))
    println("ridSidAidM size:" + ridSidAidM.size + ",\tfirst item:" + (if (ridSidAidM.size>0) ridSidAidM.iterator.next else "null"))
    println("aidsidResourcesM size:" + aidsidResourcesM.size + ",\tfirst item:" + (if (aidsidResourcesM.size>0) aidsidResourcesM.iterator.next else "null"))
    println("ridPcodeM size:" + ridPcodeM.size + ",\tfirst item:" + (if (ridPcodeM.size>0) ridPcodeM.iterator.next else "null"))
    println("newmvS size:" + newmvS.size + ",\tfirst item:" + (if (newmvS.size>0) newmvS.iterator.next else "null"))
    println("newconcertS size:" + newconcertS.size + ",\tfirst item:" + (if (newconcertS.size>0) newconcertS.iterator.next else "null"))

    val resourcetype_to_examine = "topic"
    println("sidResourcesM size 4 " + resourcetype_to_examine + ":" + sidResourcesM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size + ",\tfirst item:" + (if (sidResourcesM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size>0) sidResourcesM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).iterator.next else "null"))
    println("aidResourcesM size 4 " + resourcetype_to_examine + ":" + aidResourcesM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size + ",\tfirst item:" + (if (aidResourcesM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size>0) aidResourcesM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).iterator.next else "null"))
    println("kwdResourceM size 4 " + resourcetype_to_examine + ":" + kwdResourceM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size + ",\tfirst item:" + (if (kwdResourceM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size>0) kwdResourceM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).iterator.next else "null"))
    println("resourceKwdM size 4 " + resourcetype_to_examine + ":" + resourceKwdM.filter(_._1.contains(resourcetype_to_examine)).size + ",\tfirst item:" + (if (resourceKwdM.filter(_._1.contains(resourcetype_to_examine)).size > 0) resourceKwdM.filter(_._1.contains(resourcetype_to_examine)).iterator.next else "null"))
    //    println("tagResourceM size 4 " + resourcetype_to_examine + ":" + tagResourceM.filter(_._1.contains(resourcetype_to_examine)).size + ",\tfirst item:" + (if (tagResourceM.filter(_._1.contains(resourcetype_to_examine)).size > 0) tagResourceM.filter(_._1.contains(resourcetype_to_examine)).iterator.next else "null"))
    //    println("resourceTagM size 4 " + resourcetype_to_examine + ":" + resourceTagM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size + ",\tfirst item:" + (if (resourceTagM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).size>0) resourceTagM.filter(_._2.filter(_.contains(resourcetype_to_examine)).size > 0).iterator.next else "null"))

    val aidsidResourcesMBroad = sc.broadcast(aidsidResourcesM)
    val aidResourcesMBroad = sc.broadcast(aidResourcesM)
    val ridSidAidMBroad = sc.broadcast(ridSidAidM)
    val newmvSBroad = sc.broadcast(newmvS)
    val newconcertSBroad = sc.broadcast(newconcertS)

    //////////// 只加载aid有视频的songid
    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    println("songArtInput:" + songArtInput)
    val sidAidsM = mutable.HashMap[String, String]()
    val aidNameM = mutable.HashMap[String, String]()
    getSongArtInfoM(songArtInput, sidAidsM, aidNameM, aidVideosM, sidVideosM, aidResourcesM, sidResourcesM)
    println("sidAidsM size:" + sidAidsM.size)
    println("sid=186010(周杰伦轨迹), aids=" + sidAidsM.get("186010"))
    val sidAidsMBroad = sc.broadcast(sidAidsM)
    val aidNameMBroad = sc.broadcast(aidNameM)


    val keywordsInput = cmd.getOptionValue("mainpage_keywords")
    println("load keywords:" + keywordsInput)
    val keywords = getKeywords(keywordsInput)
    println("keywords size:" + keywords.size)
    val keywordsBroad = sc.broadcast(keywords)

    val videoTagsInput = cmd.getOptionValue("video_tags")
    println("videoTagsInput:" + videoTagsInput)
    val (keywordVideosM, videoKeywordsM, tagVideosM, videoTagsM) = getKeywordVideosM(videoTagsInput, keywords, vidTitleM, videoPredM)
    val tagVideosM4KwdScene = tagVideosM.filter(tup=> tup._2.size>=5)
    println("keywordVideosM size:" + keywordVideosM.size)
    /*println("videos for 宅舞")
    println(keywordVideosM.get("宅舞").get(0))
    println(keywordVideosM.get("宅舞").get(1))
    for ((key, vs) <- keywordVideosM) {
      println(key +"->" + vs.size)
    }

    println("tagVideosM size:" + tagVideosM.size)
    println("videos for 宅舞")
    println(tagVideosM.get("宅舞").get(0))
    println(tagVideosM.get("宅舞").get(1))
    println("tagVideosM4KwdScene size:" + tagVideosM4KwdScene.size)
    println("videos for 宅舞")
    println(tagVideosM4KwdScene.get("宅舞").get(0))
    println(tagVideosM4KwdScene.get("宅舞").get(1))*/
    /*for ((key, vs) <- tagVideosM) {
      println(key +"->" + vs.size)
    }*/
    // 将keywords中符合要求的vid输出到output/keyword_video
    flushTopRcmdData(keywordVideosM, cmd.getOptionValue("output") + "/keywords", cmd.getOptionValue("output") + "/keyword_video", vidTitleM)
    // 将tags中符合要求的vid输出到output/tag_video
    flushTopRcmdData(tagVideosM, cmd.getOptionValue("output") + "/keywords", cmd.getOptionValue("output") + "/tag_video", vidTitleM)

    /*println("videoKeywordsM size:" + videoKeywordsM.size)
    println(keywordVideosM.get("宅舞").get(0)._1 + ":" + videoKeywordsM(keywordVideosM.get("宅舞").get(0)._1))

    println("videoTagsM size:" + videoTagsM.size)
    println(tagVideosM.get("宅舞").get(0)._1 + ":" + videoTagsM(tagVideosM.get("宅舞").get(0)._1))*/

    val keywordVideosMBroad = sc.broadcast(keywordVideosM)
    val videoKeywordsMBroad = sc.broadcast(videoKeywordsM)
    val tagVideosMBroad = sc.broadcast(tagVideosM)
    val tagVideosM4KwdSceneBroad = sc.broadcast(tagVideosM4KwdScene)
    val videoTagsMBroad = sc.broadcast(videoTagsM)
    println("keys...")
    println(keywordVideosM.keys)

    println("Load 推荐过视频")
    val recedVideosPath = cmd.getOptionValue("reced_videos")
    println(recedVideosPath)
    val recedVideoData = spark.read.textFile(recedVideosPath).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t", 2)
      if (ts(0).equals("359792224")) {
        println("RECED KEYWORD reced:" +  line)
      }
      (ts(0), ts(1))

    }).toDF("uid", "recedVideos")


    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsRecedKeywordDataPaths = getExistsPath(cmd.getOptionValue("reced_keyword"), fs)
    val recedKeywordData = spark.sparkContext.textFile(existsRecedKeywordDataPaths.mkString(",")).map(line => {
      // keyword: recommendimpress	mainpage	android	129232837	  1520170808000	388455	video	  2	          bySort	null	      -1	      simTagv3-1447101-video	-1	      电视剧
      // guess  : recommendimpress	mainpage	android	1292328832	1520147571000	1428679	video	  1	          hot	    null	      -1	      null	                  -1	      猜你喜欢
      // format : action            page      os      uid         logTime       vid     vType   position    alg     netstatus   sourceId  sourceInfo              groupId   groupName
      // num    : 1                 2         3       4           5             6       7        8          9       10          11        12                      13        14
      val info = line.split("\t")
      val actionUserId = info(3)
      val videoId = info(5)
      var vType = info(6)
      vType = vType.replaceAll("eventactivity", "evtTopic")
      val idtype = videoId + "-" + vType
      var logTime = 0L
      var lastRecedFromNow = 0
      if (info.length >= 5) {
        logTime = info(4).toLong
        lastRecedFromNow = getRecedDayFromNow(NOW_TIME, logTime)
        if (lastRecedFromNow < 0)
          lastRecedFromNow *= -1
      }
      var keyword = ""
      if (info.length >= 14) {
        keyword = info(13)
      }
      (actionUserId, (keyword, idtype + ":" + lastRecedFromNow))
    }).groupByKey.map({case (key, values) =>
      val keywords = mutable.HashSet[String]()
      val kwdImpressedVideos = mutable.HashSet[String]()
      val guessImpressedVideos = mutable.HashSet[String]()
      for (value <- values) {
        val kwd = value._1
        val idtypeDay = value._2
        val lastRecedFromNow = idtypeDay.split(":")(1).toInt
        if (!kwd.isEmpty) {
          if (!kwd.equals("猜你喜欢")) {
            if (lastRecedFromNow <= KWD_REC_WINDOW)
              keywords.add(kwd)
            if (!idtypeDay.isEmpty)
              kwdImpressedVideos.add(idtypeDay)
          } else {
            if (!idtypeDay.isEmpty)
              guessImpressedVideos.add(idtypeDay)
          }
        }
      }
      if (key.equals("135571358")) {
        println("reced info from mainpage log 4 135571358: \nrecedKeywords:" + keywords.mkString(",") + "\nkwdImpressedVideos:" + kwdImpressedVideos.mkString(",") + "\nguessImpressedVideos:" + guessImpressedVideos.mkString(","))
      }
      (key, keywords.mkString(","), kwdImpressedVideos.mkString(","), guessImpressedVideos.mkString(","))
    }).toDF("uid", "recedKeywords", "impressedKwdVideos", "impressedGuessVideos")


    println("Load algMerge")
    val mlResortInput = cmd.getOptionValue("algMerge")
    val algMergeResultData = spark.read.option("sep", "\t").csv(mlResortInput.split(",").toSeq : _*)/*.map(line => {
      // 571458196	538610:video:songNotActive_songNotActive-31719414-song&m9
      val ts = line.split("\t", 2)
      (ts(0), ts(1))
    })*/.toDF("uid", "algMergeResult")

    println("Load reced")
    //TODO

    println("Load user province info")
    val uidProvince = spark.read.textFile(cmd.getOptionValue("Music_UserProfile2"))
      .filter(line => {
        val info = line.split("\t")
        if (info.length >= 7 && info(6).toLong > 0)
          true
        else
          false
      })
      .map(line => {
        val info = line.split("\t")
        (info(0), info(6))
      }).toDF("uid", "provinceCode")

    println("Load songPref")
    val uidSongPref = spark.read.textFile(cmd.getOptionValue("songPref"))
      .map(line => {
        // uid.toString  \t  subSongs.mkString(",")  \t  searchSongs.mkString(",")  \t  localSongs.mkString(",")
        val us = line.split("\t", 2)
        (us(0), us(1))
      }).toDF("uid", "songPref")

    println("Load user_sub_art")
    val uidPrefArt = spark.read.textFile(cmd.getOptionValue("user_sub_art"))
      .map(line => {
        // ArtistId  \t  UserId  \t  SubTime
        val ts = line.split("\t")
        if (ts.length >= 3) {
          (ts(1), ts(0))
        } else {
          (null, null)
        }
      }).filter(_._1 != null).rdd.groupByKey.map({ case (key, values) =>
      val aidsBuff = mutable.ArrayBuffer[String]()
      for (value <- values) {
        aidsBuff.append(value)
      }
      if (key.equals("359792224")) {
        println("pref arts:" +  aidsBuff.mkString(","))
      }
      (key, aidsBuff.mkString(","))
    }).toDF("uid", "prefAids")

    //TODO
    // val sidAidTagM = mutable.HashMap[String, String]()
    // val sidAidMBroad = sc.broadcast(sidAidM)
    //val aidNameTagM = mutable.HashMap[String, String]()
    // val aidNameTagMBroad = sc.broadcast(aidNameTagM)

    /*// 测试用户
    var testUids = Set[String]()
    for (uid <- TEST_USER_STR.split(",")) {
      testUids += uid
    }
    val testUidsBroad = sc.broadcast(testUids)*/

    val rcmdData = uidSongPref
      .join(uidPrefArt, Seq("uid"), "outer")
      .join(algMergeResultData, Seq("uid"), "outer")
      .join(uidProvince, Seq("uid"), "left_outer")
      .join(recedVideoData, Seq("uid"), "left_outer")
      .join(recedKeywordData, Seq("uid"), "left_outer")
      .flatMap(row => {
        val uid = row.getAs[String]("uid")
        //if (testUidsBroad.value.contains(uid) || uid.toLong % 10 == 8) {
        var uidPcode = row.getAs[String]("provinceCode")
        if (uidPcode == null)
          uidPcode = "-1"
        // (1) 加载喜欢的歌曲
        // 从song中推测出的用户喜欢的artist：subSong、localSong、searchSong、subArtist
        val aidSidsM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
        //songid:type:aid:aname,... \t search \t local
        val songPref = row.getAs[String]("songPref")
        if (songPref != null) {
          val ss = songPref.split("\t", 3)
          appendAidSidsM(ss(0), aidSidsM, sidAidsMBroad.value) // sub
          if (ss.length >= 2) {
            appendAidSidsM(ss(1), aidSidsM, sidAidsMBroad.value) // search
          }
          if (ss.length >= 3) {
            appendAidSidsM(ss(2), aidSidsM, sidAidsMBroad.value) // local
          }
        }

        val aidsPref = row.getAs[String]("prefAids") // 收藏艺人
        if (aidsPref != null) {
          appendAidsM(aidsPref, aidSidsM)
        }
        // 按照song数量由多到少排序
        val aidSidsSorted = resortM(aidSidsM)
        //println("aidSidsSorted:" + aidSidsSorted)

        // (2) 加载推荐过数据
        val impressedKwdVideos = row.getAs[String]("impressedKwdVideos")
        val impressedGuessVideos = row.getAs[String]("impressedGuessVideos")
        val recedVideos = row.getAs[String]("recedVideos")
        val recedKeywords = row.getAs[String]("recedKeywords")
        // uid \t 周杰伦:aid:1362297:video:mArt:猜你喜欢,...  1362297:video:mArt:猜你喜欢,
        val recedAidOrKeywords = mutable.HashSet[String]()
        val recedResroucetypeAidOrKeywordsM = mutable.HashMap[String, Int]()
        val recedResources = mutable.HashSet[String]()
        val recedVids = mutable.HashSet[String]()
        val recedSids = mutable.HashSet[String]()
        // TODO 去重关键词短一点， sid时间长一点
        getRecedKeyVideosFromLog(recedKeywords, impressedKwdVideos, impressedGuessVideos,
          recedVideos, recedAidOrKeywords, recedResources, recedVids, recedSids,
          vidSidAidMBroad.value, ridSidAidMBroad.value)
        if (uid.equals("135571358")) {
          println("for 135571358")
          println("recedAidOrKeywords:" + recedAidOrKeywords)
          println("recedResources:" + recedResources)
          println("recedSids:" + recedSids)
          println("recedVids:" + recedVids)
        } else if (uid.equals("627403")) {
          println("for 627403")
          println("recedGuessVideos:" + impressedGuessVideos)
          println("recedAidOrKeywords:" + recedAidOrKeywords)
          println("recedResources:" + recedResources)
          println("recedSids:" + recedSids)
          println("recedVids:" + recedVids)
        }
        //}
        //println("aidSidsSorted:" + aidSidsSorted)

        // (3) 音乐相关关键词
        val curRecedIdtype = mutable.HashSet[String]()

        val random = new Random()
        val rcmdsForArtCands = mutable.ArrayBuffer[String]() // 推荐候选集
        val resourcetypeResrouceM = mutable.Map[String, ArrayBuffer[String]]()  // 音乐相关single resource召回
        val kwdResourcetypesetM = mutable.HashMap[String, mutable.HashSet[String]]() //
        if (uid.equals("135571358")) {
          println("aid for uid:135571358")
          println(aidSidsSorted.map(tup => tup._1 + ":" + tup._2.mkString(",")).mkString("\n"))
        }
        val asIter = aidSidsSorted.iterator
        while (asIter.hasNext &&
          (rcmdsForArtCands.size < CAND_MIN_NUM_FOR_SINGLE_KEY || resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE))
        ) {
          // for ((aid, sids) <- aidSidsM) {
          val (aid, sids) = asIter.next()
          var aname = aidNameMBroad.value.getOrElse(aid, null)
          if (aname != null && !aname.equals("周杰伦") && !recedAidOrKeywords.contains(aid) && !recedAidOrKeywords.contains(aname)) {
            if (random.nextInt(2) < 1) { // 1/2
              // 关联音乐偏好 aid + sid
              if (uid.equals("135571358")) {
                println("aname:" + aname)
              }
              if (aname != null && random.nextInt(2) < sids.size) { // 当sids.size==1时，以1/2概率跳过；当sids.size>1时，一定进入该逻辑

                getRcmd(aid, aname, sids,
                  rcmdsForArtCands, resourcetypeResrouceM,
                  recedSids, recedResources, recedVids, recedResroucetypeAidOrKeywordsM,
                  kwdResourcetypesetM,
                  vidTitleMBroad.value,
                  vidSidAidMBroad.value, ridSidAidMBroad.value,
                  aidsidResourcesMBroad.value, aidsidVideosMBroad.value,
                  aidResourcesMBroad.value, aidVideosMBroad.value, ridPcodeM,
                  musicVideoSBroad.value, newmvSBroad.value, newconcertSBroad.value,
                  CAND_MAX_NUM_FOR_SINGLE_KEY, "mart",
                  uidPcode)
                if (uid.equals("359792224")) {
                  println("rcmdsForArtCands 0:" + rcmdsForArtCands)
                }
              }

              if (rcmdsForArtCands.size < CAND_MIN_NUM_FOR_SINGLE_KEY) {
                rcmdsForArtCands.clear() // 艺人召回不满足最低要求，忽略 TODO 多艺人
              }
            } else { // 多个艺人
              if (random.nextInt(2) < 1) { // 1/2概率
                val (keyword, simaids, maxScore) = getKwordSimaids(aid, aidSidsM, artSimsMBroad.value, artTagsMBroad.value, kwdTagMBrod.value, recedAidOrKeywords)
                if (uid.equals("135571358")) {
                  println("kwd simaids 4 135571358:")
                  println("keyword:" + keyword)
                  println("simaids:" + simaids)
                  println("maxScore:" + maxScore)
                }
                if (keyword != null && !keyword.isEmpty && simaids != null && simaids.size > 1) {
                  for (said <- simaids if rcmdsForArtCands.size < CAND_MAX_NUM_FOR_SINGLE_KEY) {
                    var resNum = rcmdsForArtCands.size + 1 // 正常情况，每个artist一条召回
                    if (maxScore <= 1 && rcmdsForArtCands.isEmpty) { // 对于偏好比较小的，第一个art出2条
                      resNum = 2
                    }
                    if (!recedAidOrKeywords.contains(aid) && !recedAidOrKeywords.contains(aname)) {
                      getRcmd(said, keyword, sids,
                        rcmdsForArtCands, resourcetypeResrouceM,
                        recedSids, recedResources, recedVids, recedResroucetypeAidOrKeywordsM,
                        kwdResourcetypesetM,
                        vidTitleMBroad.value,
                        vidSidAidMBroad.value, ridSidAidMBroad.value,
                        aidsidResourcesMBroad.value, aidsidVideosMBroad.value,
                        aidResourcesMBroad.value, aidVideosMBroad.value, ridPcodeM,
                        musicVideoSBroad.value, newmvSBroad.value, newconcertSBroad.value,
                        resNum, "mtag",
                        uidPcode)
                    }
                  }
                  if (rcmdsForArtCands.size < CAND_MIN_NUM_FOR_SINGLE_KEY) { // 不够，再重复取一遍
                    for (said <- simaids if rcmdsForArtCands.size < CAND_MAX_NUM_FOR_SINGLE_KEY) {
                      var resNum = rcmdsForArtCands.size + 1
                      if (maxScore <= 1 && rcmdsForArtCands.isEmpty) { // 对于偏好比较小的，第一个art出2条
                        resNum = 2
                      }
                      if (!recedAidOrKeywords.contains(aid) && !recedAidOrKeywords.contains(aname)) {
                        getRcmd(said, keyword, sids,
                          rcmdsForArtCands, resourcetypeResrouceM,
                          recedSids, recedResources, recedVids, recedResroucetypeAidOrKeywordsM,
                          kwdResourcetypesetM,
                          vidTitleMBroad.value,
                          vidSidAidMBroad.value, ridSidAidMBroad.value,
                          aidsidResourcesMBroad.value, aidsidVideosMBroad.value,
                          aidResourcesMBroad.value, aidVideosMBroad.value, ridPcodeM,
                          musicVideoSBroad.value, newmvSBroad.value, newconcertSBroad.value,
                          resNum, "mtag",
                          uidPcode)
                      }
                    }
                  }
                }
                if (uid.equals("135571358")) {
                  println("rcmdsForArtCands 4 135571358:" + rcmdsForArtCands)
                }
                if (rcmdsForArtCands.size < CAND_MIN_NUM_FOR_SINGLE_KEY) {
                  rcmdsForArtCands.clear() // 艺人召回不满足最低要求，忽略 TODO 多艺人
                }
              }
            }
          }
        }

        val result = mutable.ArrayBuffer[(String, String)]()

        // (4) 视频关键词 + 非关键词
        val algMergeResult = row.getAs[String]("algMergeResult")
        if (algMergeResult != null) {
          val (rcmdsForKeywordCands, guesslikeCands) =
            getTopKeywordVideoM(algMergeResult, recedVids, recedResources, recedSids, recedAidOrKeywords, recedResroucetypeAidOrKeywordsM,
              kwdResourcetypesetM,
              keywordsBroad.value, curRecedIdtype, vidSidAidMBroad.value, ridSidAidMBroad.value,
              videoKeywordsMBroad.value, keywordVideosMBroad.value,
              videoTagsMBroad.value, tagVideosM4KwdSceneBroad.value, tagVideosMBroad.value,
              resourceKwdMBroad.value, kwdResourceMBroad.value,
              //                                  resourceTagMBroad.value, tagResourceMBroad.value,
              vidTitleMBroad.value, musicVideoSBroad.value, newmvSBroad.value, newconcertSBroad.value,
              CAND_MAX_NUM_FOR_SINGLE_KEY, CAND_MIN_NUM_FOR_SINGLE_KEY, CAND_MAX_NUM_FOR_GUESS,
              rcmdsForArtCands, resourcetypeResrouceM,
              uidPcode, ridPcodeM,
              uid)
          rcmdsForArtCands.appendAll(rcmdsForKeywordCands)

          if (!rcmdsForArtCands.isEmpty) {
            // ridType 随机化pos
            val finalResult = rcmdsForArtCands.filter(res => res.split(":")(2).equals("video"))
            rcmdsForArtCands.filter(res => !res.split(":")(2).equals("video"))
              .foreach(res => {
                val pos = random.nextInt(finalResult.length + 1)
                finalResult.insert(pos, res)
              })

            val feature_result = finalResult.filter(res => res.split(":")(2).endsWith("_f"))
            val alg_result = finalResult.filter(res => !res.split(":")(2).endsWith("_f"))


            if (alg_result.size > 0)
              result.append(("A", uid + "\t" + alg_result.mkString(",")))
            if (feature_result.size > 0) {
              result.append(("F", uid + "\t" + feature_result.map { line =>
                // 梁静茹-8325:21918090:evtTopic:martsong:254485-song
                val info = line.split(":")
                val id = info(1)
                val resourcetype = info(2)
                val score = 1.0
                id + ":" + score + ":" + parseResourcetype2Blocktype(resourcetype)
              }.mkString(",")))
            }
          }

          if (!guesslikeCands.isEmpty) {
            result.append(("R", uid + "_video" + "\t" + guesslikeCands.mkString(",")))
          }
          val feature_result = ArrayBuffer[String]()
          for (entry <- resourcetypeResrouceM) {
            val resourcetype = entry._1
            if (resourcetype.endsWith("_f"))
              feature_result.appendAll(entry._2)
            else if (entry._2.size > 0)
              result.append(("R", uid + "_" + entry._1 + "\t" + entry._2.mkString(",")))
          }
          // 运营相关推荐
          if (feature_result.size > 0) {
            result.append(("F", uid + "\t" + feature_result.map{line =>
              val info = line.split(":")
              val id = info(0)
              val resourcetype = info(1)
              val score = 1.0
              id + ":" + score + ":" + parseResourcetype2Blocktype(resourcetype)
            }.mkString(",")))
          }

        } else {
          if (!rcmdsForArtCands.isEmpty) {
            // ridType 随机化pos
            val finalResult = rcmdsForArtCands.filter(res => res.split(":")(2).equals("video"))
            rcmdsForArtCands.filter(res => !res.split(":")(2).equals("video"))
              .foreach(res => {
                val pos = random.nextInt(finalResult.length + 1)
                finalResult.insert(pos, res)
              })

            val feature_result = finalResult.filter(res => res.split(":")(2).endsWith("_f"))
            val alg_result = finalResult.filter(res => !res.split(":")(2).endsWith("_f"))

            if (alg_result.size > 0)
              result.append(("A", uid + "\t" + alg_result.mkString(",")))
            if (feature_result.size > 0) {
              result.append(("F", uid + "\t" + feature_result.map { line =>
                // 梁静茹-8325:21918090:evtTopic:martsong:254485-song
                val info = line.split(":")
                val id = info(1)
                val resourcetype = info(2)
                val score = 1.0
                id + ":" + score + ":" + parseResourcetype2Blocktype(resourcetype)
              }.mkString(",")))
            }
          }
        }
        result
        /*} else {
          mutable.ArrayBuffer[(String, String)]()
        }*/
      })
      .filter(_ != null).filter(_ != "").filter(_ != None)
      .cache
    // 使用partitionBy的作用：为了改名字（使用part-*****这种类型的名字）
    rcmdData.filter(_._1.equals("A")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_keyword_video")
    rcmdData.filter(_._1.equals("R")).map({case(stype,line) => (line.toString.split("_",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_resource")
    rcmdData.filter(_._1.equals("F")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_feature")
    /*rcmdData.filter(_._1.equals("GnewConcert")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_newConcert")
    rcmdData.filter(_._1.equals("Gconcert")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_concert")
    rcmdData.filter(_._1.equals("GnewMv")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_newMv")
    rcmdData.filter(_._1.equals("Gmv")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_mv")
    rcmdData.filter(_._1.equals("GevtTopic")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_evtTopic")
    rcmdData.filter(_._1.equals("Gvideo")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_video")
    // topic不推荐
    //rcmdData.filter(_._1.equals("Gtopic")).map({case(stype, line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/user_guess_topic")
    */
  }

  def isSongReced(idRelativeSids: mutable.ArrayBuffer[String], recedSids:mutable.HashSet[String]):Boolean = {
    var isSongRecedVideo = false
    if (idRelativeSids != null) {
      for (sid <- idRelativeSids) {
        if (recedSids.contains(sid))
          isSongRecedVideo = true
      }
    }
    isSongRecedVideo
  }

  def addRecedSong(idRelativeSids: mutable.ArrayBuffer[String], recedSids:mutable.HashSet[String]) = {

    if (idRelativeSids != null) {
      for (sid <- idRelativeSids) {
        recedSids.add(sid)
      }
    }
  }

  def getTopKeywordVideoM(algMergeResult: String, recedVids: mutable.HashSet[String], recedResources: mutable.HashSet[String], recedSids: mutable.HashSet[String], recedAidOrKeywords: mutable.HashSet[String], recedResroucetypeAidOrKeywordsM: mutable.HashMap[String, Int],
                          kwdResourcetypesetM: mutable.HashMap[String, mutable.HashSet[String]], // 每个关键词包含的resourceSet
                          keywords: mutable.HashSet[String], curRecedIdtype: mutable.HashSet[String],
                          vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                          ridSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                          videoKeywordsM: mutable.HashMap[String, mutable.ArrayBuffer[String]], keywordVideoM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
                          videoTagsM: mutable.HashMap[String, mutable.ArrayBuffer[String]], tagVideosM4KwdScene: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]], tagVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
                          resourceKwdsM: mutable.HashMap[String, mutable.ArrayBuffer[String]],  kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                          //                          resourceTagsM: mutable.HashMap[String, mutable.ArrayBuffer[String]],  tagResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                          videoTitleM: mutable.HashMap[String, String], musicVidS: Set[String], newmvS: mutable.Set[String], newconcertS: mutable.Set[String],
                          maxNum: Int, minNum: Int, maxGuessNum: Int,
                          rcmdsForArtCands: mutable.ArrayBuffer[String], resourcetypeResrouceM: mutable.Map[String, ArrayBuffer[String]],
                          uidPcode: String, ridPcodeM: mutable.HashMap[String, String],
                          uid: String) = {

    val rcmdsForKeywordCands = mutable.ArrayBuffer[String]()
    //var resourcetypeResrouceM = mutable.Map[String, ArrayBuffer[String]]()
    val rcmdsForGuessCands = mutable.ArrayBuffer[String]()

    val rcmdvidReasonM = mutable.LinkedHashMap[String, String]()
    var kwdRcvideosM = Map[String, mutable.ArrayBuffer[String]]()
    var tagRcvideosM = Map[String, mutable.ArrayBuffer[String]]()

    // 计算algMerge中用户对keyword的偏好
    val vs = algMergeResult.split(",")
    for (vreason <- vs) {
      // 571458196	538610:video:songNotActive_songNotActive-31719414-song&m9
      val rcmdResons = vreason.split("_", 2)
      val idTypeAlg = rcmdResons(0).split(":")
      if (idTypeAlg(1).equals("video")) { // 只处理了视频
        // 将用户algMerge结果按kwd分组收集到kwdRcvideosM
        val kwds = videoKeywordsM.getOrElse(idTypeAlg(0), null)
        if (kwds != null) {
          for (kwd <- kwds) {
            if (!recedAidOrKeywords.contains(kwd)) {
              var rcvideos = kwdRcvideosM.getOrElse(kwd, null)
              if (rcvideos == null) {
                rcvideos = mutable.ArrayBuffer[String]()
                kwdRcvideosM += (kwd -> rcvideos)
              }
              rcvideos.append(idTypeAlg(0))
            }
          }
        }
        // 将用户algMerge结果按tag分组收集到tagRcvideosM
        val tags = videoTagsM.getOrElse(idTypeAlg(0), null)
        if (tags != null) {
          for (tag <- tags) {
            if (!recedAidOrKeywords.contains(tag)) {
              var rcvideos = tagRcvideosM.getOrElse(tag, null)
              if (rcvideos == null) {
                rcvideos = mutable.ArrayBuffer[String]()
                tagRcvideosM += (tag -> rcvideos)
              }
              rcvideos.append(idTypeAlg(0))
            }
          }
        }
      }

      // 将用户algMerge结果按vid,firstReasonInfo保存到rcmdvidReasonM
      if (rcmdResons.length >= 2) {
        // songNotActive-31719414-song&m9
        val reasons = rcmdResons(1).split("&")
        if (reasons.length >= 1) {
          if (reasons(0).contains("-")) {
            rcmdvidReasonM.put(idTypeAlg(0), reasons(0))
          }
        }
      }
    }

    var recedKwdS = Set[String]()
    if (!kwdRcvideosM.isEmpty) {
      val kwdCntArray = kwdRcvideosM.toArray[(String, mutable.ArrayBuffer[String])]
      val sortedKwdCntArray = kwdCntArray.sortWith(_._2.size > _._2.size)
      val kwdIter = sortedKwdCntArray.iterator

      if (uid.equals("135571358")) {
        println("guessInfo 4 135571358:\tsortedKwdCntArray:" + sortedKwdCntArray.mkString(",") + ",\nrcmdvidReasonM:" + rcmdvidReasonM.toString)
      }
      var alreadyRecedKwdCount = 0
      val random = new Random()
      while (kwdIter.hasNext && alreadyRecedKwdCount < 2) { // maxNum * 2；出2个keyword
        val rcmds4SingleKwdCands = mutable.ArrayBuffer[String]()
        var resourcesMatchCnt = 0
        val (kwd, videos) = kwdIter.next
        val kwdContainResourcetypeS = kwdResourcetypesetM.getOrElse(kwd, mutable.HashSet[String]())
        if (kwdContainResourcetypeS.isEmpty)
          kwdResourcetypesetM.put(kwd, kwdContainResourcetypeS)
        // 如果有resources关联结果，优先匹配KWD_RESOURCE_THRED个
        if (rcmds4SingleKwdCands.size < maxNum && kwdResourceM.contains(kwd)) {
          val resources = kwdResourceM(kwd)
          var startIdx = random.nextInt(resources.length)
          while (startIdx < resources.length && resourcesMatchCnt < KWD_RESOURCE_THRED) {
            val ridType = resources(startIdx)
            if (!recedResources.contains(ridType)) {
              var ridPcode = "-1"
              if (ridPcodeM.contains(ridType))
                ridPcode = ridPcodeM(ridType)
              // 演唱会同城过滤
              if (!ridType.endsWith("concert") || (ridType.endsWith("concert") && ridPcode.equals(uidPcode))) {

                val resourcetype = ridType.split("-")(1)
                // 关键词资源多样性
                if (!kwdContainResourcetypeS.contains(resourcetype)) {
                  val resourcetypeAidOrKwd = resourcetype + "-" + kwd
                  val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
                  if (cnt < GUESS_RESOURCE_THRED) {
                    rcmds4SingleKwdCands.append(kwd + ":" + ridType.replaceAll("-", ":") + ":bySort")
                    recedResources.add(ridType)
                    kwdContainResourcetypeS.add(resourcetype)
                    resourcesMatchCnt += 1
                    recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
                  }
                }
              }
            }
            startIdx += 1
          }
        }
        // algMerge推荐videos
        val videosIter = videos.iterator
        while (videosIter.hasNext && rcmds4SingleKwdCands.size < maxNum) {

          val vid = videosIter.next
          val (sids, aids) = vidSidAidM.getOrElse(vid, (null, null))
          if (!recedVids.contains(vid) &&
            !isSongReced(sids, recedSids) // 单曲去重
          /*!curRecedIdtype.contains(vid + ":video")*/ ) { // TODO 过滤art sid等
            //curRecedIdtype.add(vid + ":video")
            if (videoTitleM.contains(vid) && musicVidS.contains(vid)) {

              val resourcetype = "video"
              val entities = ArrayBuffer[String]()
              if (aids != null)
                entities.appendAll(aids)
              entities.append(kwd)
              if (isFilteredByAidOrKwd(resourcetype, entities, GUESS_RESOURCE_THRED, recedResroucetypeAidOrKeywordsM)) {
                recedVids.add(vid)
                addRecedSong(sids, recedSids)
                val reason = rcmdvidReasonM.getOrElse(vid, null)
                if (reason != null && reason != None) {
                  rcmds4SingleKwdCands.append(kwd + ":" + vid + ":video:bySort:" + reason)
                } else {
                  rcmds4SingleKwdCands.append(kwd + ":" + vid + ":video:bySort")
                }
                addAidOrKwd(resourcetype, entities, recedResroucetypeAidOrKeywordsM)
              }
            }
          }
        }
        // 关键词下离线不够，keyword补充推荐
        if (rcmds4SingleKwdCands.size < maxNum) {
          val vidWtArray: mutable.ArrayBuffer[(String, Float)] = keywordVideoM.getOrElse(kwd, null)
          if (vidWtArray != null) {
            val vidWtIter = vidWtArray.iterator
            // kwd下的video排序按照rawPrediction
            while (vidWtIter.hasNext && rcmds4SingleKwdCands.size < maxNum) { // 同一个keyword
              val (vid, wtt) = vidWtIter.next
              val (sids, aids) = vidSidAidM.getOrElse(vid, (null, null))
              if (!recedVids.contains(vid) &&
                !isSongReced(sids, recedSids)
              /*!curRecedIdtype.contains(vid + ":video")*/ ) { // TODO 过滤art sid等
                // curRecedIdtype.add(vid + ":video")
                if (videoTitleM.contains(vid) && musicVidS.contains(vid)) {

                  val resourcetype = "video"
                  val entities = ArrayBuffer[String]()
                  if (aids != null)
                    entities.appendAll(aids)
                  entities.append(kwd)
                  if (isFilteredByAidOrKwd(resourcetype, entities, GUESS_RESOURCE_THRED, recedResroucetypeAidOrKeywordsM)) {
                    recedVids.add(vid)
                    addRecedSong(sids, recedSids)
                    val reason = rcmdvidReasonM.getOrElse(vid, null)
                    if (reason != null && reason != None) {
                      rcmds4SingleKwdCands.append(kwd + ":" + vid + ":video:bySortEx:" + reason)
                    } else {
                      rcmds4SingleKwdCands.append(kwd + ":" + vid + ":video:bySortEx")
                    }
                    addAidOrKwd(resourcetype, entities, recedResroucetypeAidOrKeywordsM)
                  }
                }
              }
            }
          }
        }
        // 还不够，使用resourceKwd补足
        if (rcmds4SingleKwdCands.size < maxNum && kwdResourceM.contains(kwd)) {
          val resources = kwdResourceM(kwd)
          for (idx <- 0 until (resources.length) if rcmds4SingleKwdCands.size < maxNum) { // TODO 后续可以改成random获取
            val ridType = resources(idx)
            val (sids, aids) = ridSidAidM.getOrElse(ridType, (null, null))
            if (!recedResources.contains(ridType) &&
              !isSongReced(sids, recedSids)
            ) {
              var ridPcode = "-1"
              if (ridPcodeM.contains(ridType))
                ridPcode = ridPcodeM(ridType)
              // 演唱会同城过滤
              if (!ridType.endsWith("concert") || (ridType.endsWith("concert") && ridPcode.equals(uidPcode))) {

                val resourcetype = ridType.split("-")(1)
                // 关键词资源多样性
                if (!kwdContainResourcetypeS.contains(resourcetype)) {
                  val entities = ArrayBuffer[String]()
                  if (aids != null)
                    entities.appendAll(aids)
                  entities.append(kwd)
                  if (isFilteredByAidOrKwd(resourcetype, entities, GUESS_RESOURCE_THRED, recedResroucetypeAidOrKeywordsM)) {
                    rcmds4SingleKwdCands.append(kwd + ":" + ridType.replaceAll("-", ":") + ":bySort")
                    recedResources.add(ridType)
                    kwdContainResourcetypeS.add(resourcetype)
                    addRecedSong(sids, recedSids)
                    addAidOrKwd(resourcetype, entities, recedResroucetypeAidOrKeywordsM)
                  }
                }
              }
            }
          }
        }
        // rcmds4SingleKwdCands数量满足最低要求，rcmdsForKeywordCands
        if (rcmds4SingleKwdCands.size >= minNum) {
          rcmdsForKeywordCands.appendAll(rcmds4SingleKwdCands)
          alreadyRecedKwdCount += 1
          if (recedKwdS.isEmpty || // 第一个关键词
            (recedKwdS.size > 0 && rcmdsForArtCands.isEmpty)) // TODO 第二个关键词做候补时，不需要放入recedKwdS中以供后面的guess逻辑去重
            recedKwdS += kwd
        }
      }
    }

    // 猜你喜欢, 从剩余keyword里面取
    if (!tagRcvideosM.isEmpty) {
      val sortedTagCntArray = tagRcvideosM.toArray[(String, mutable.ArrayBuffer[String])].sortWith(_._2.size > _._2.size)
      //val tagIter = sortedTagCntArray.iterator

      if (uid.equals("135571358")) {
        println("guessInfo 4 135571358:\tsortedTagCntArray:" + sortedTagCntArray.mkString(",") + ",\nrcmdvidReasonM:" + rcmdvidReasonM.toString)
      }
      for ((tag, videos) <- sortedTagCntArray if (rcmdsForGuessCands.size < maxGuessNum || resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE))) {

        // resources关联结果
        if (kwdResourceM.contains(tag) && resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE)) {
          val resources4Tag = kwdResourceM(tag)
          //var resourcesMatchesCnt_perTag = 0     // 每个tag下最多召回RESOURCE_PER_KWD个相关reources推荐
          for (ridType <- resources4Tag) {
            val rid = ridType.split("-")(0)
            var resourcetype = ridType.split("-")(1)
            if (!recedResources.contains(ridType)) {
              var ridPcode = "-1"
              if (ridPcodeM.contains(ridType))
                ridPcode = ridPcodeM(ridType)
              // 演唱会同城过滤
              if (!ridType.endsWith("concert") || (ridType.endsWith("concert") && ridPcode.equals(uidPcode))) {

                val resourcetypeAidOrKwd = resourcetype + "-" + tag
                val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
                if (cnt < GUESS_RESOURCE_THRED) {
                  val resourceRecallA = resourcetypeResrouceM.getOrElse(resourcetype, ArrayBuffer[String]())
                  resourcetypeResrouceM += (resourcetype -> resourceRecallA)
                  //if (resourcesMatchesCnt_perTag < RESOURCE_PER_KWD) {
                  // 区分新mv和新concert
                  /*if (resourcetype.equals("mv") && newmvS.contains(rid))
                    resourcetype = "newMv"
                  else if (resourcetype.equals("concert") && newconcertS.contains(rid))
                    resourcetype = "newConcert"

                  if (resourcetype.startsWith("new"))
                    resourceRecallA.insert(0, rid + ":" + resourcetype + ":bySortGsk:" + tag)
                  else
                    */ resourceRecallA.append(rid + ":" + resourcetype + ":bySortGsk:" + tag)

                  //resourcesMatchesCnt_perTag += 1
                  recedResources.add(ridType)
                  recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
                }
              }
            }
          }
        }

        /*// algMerge推荐
        val videosIter = videos.iterator
        if (!recedKwdS.contains(tag)) {
          // 每个tag下只拿VIDEO_PER_KWD个video
          var matchVideoCnt_perKwd = 0
          while (videosIter.hasNext && rcmdsForGuessCands.size < maxGuessNum && matchVideoCnt_perKwd < VIDEO_PER_KWD) {
            val vid = videosIter.next
            if (!recedVids.contains(vid) /*!curRecedIdtype.contains(vid + ":video")*/ ) { // TODO 过滤art sid等
              //curRecedIdtype.add(vid + ":video")
              if (videoTitleM.contains(vid) && musicVidS.contains(vid)) {
                val resourcetype = "video"
                val resourcetypeAidOrKwd = resourcetype + "-" + tag
                val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
                if (cnt < GUESS_RESOURCE_THRED) {
                  recedVids.add(vid)
                  val reason = rcmdvidReasonM.getOrElse(vid, null)
                  if (reason != null && reason != None) {
                    rcmdsForGuessCands.append(vid + ":video:bySortGsk:" + reason)

                  } else {
                    rcmdsForGuessCands.append(vid + ":video:bySortGsk")
                  }
                  matchVideoCnt_perKwd += 1
                  recedKwdS += tag
                  recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
                }
              }
            }
          }
        }*/
        if (uid.equals("135571358")) {
          println("guessInfo 4 135571358 meta:tag:" + tag + ",rcmdsForGuessCands:" + rcmdsForGuessCands.mkString(","))
        }
      }
      /*if (uid.equals("135571358")) {
        println("guessInfo 4 135571358 meta:tag:out:rcmdsForGuessCands:" + rcmdsForGuessCands.mkString(","))
      }*/
      /*if (rcmdsForGuessCands.size < maxGuessNum) {
        for (i <- 0 until sortedKwdCntArray.size) { // 后12个从头开始取
          val (kwd, videos) = sortedKwdCntArray(i)
          val videosIter = videos.iterator
          // algMerge推荐
          var matchVideo = false
          while (videosIter.hasNext && rcmdsForGuessCands.size < maxGuessNum && !matchVideo) {
            val vid = videosIter.next
            if (!recedVids.contains(vid)/*!curRecedIdtype.contains(vid + ":video")*/) { // TODO 过滤art sid等
              // curRecedIdtype.add(vid + ":video")
              recedVids.add(vid)
              if (videoTitleM.contains(vid)) {
                val reason = rcmdvidReasonM.getOrElse(vid, null)
                if (reason != null && reason != None) {
                  rcmdsForGuessCands.append(vid + ":video:bySortGsk_" + reason)

                } else {
                  rcmdsForGuessCands.append(vid + ":video:bySortGsk")
                }
                matchVideo = true
              }
            }
          }
        }
      }*/
    }
    /*if(rcmdsForGuessCands.size < maxGuessNum) { // 从离线结果中不分keyword直接取
      val videosIter = rcmdvidReasonM.keys.iterator
      var cnt = 0
      while (videosIter.hasNext && rcmdsForGuessCands.size < maxGuessNum) {
        cnt = cnt + 1
        if (cnt >= 8) {
          val vid = videosIter.next
          if (!recedVids.contains(vid)/*!curRecedIdtype.contains(vid + ":video")*/) { // TODO 过滤art sid等
            if (videoTitleM.contains(vid) && musicVidS.contains(vid)) {
              // curRecedIdtype.add(vid + ":video")
              recedVids.add(vid)
              val reason = rcmdvidReasonM.getOrElse(vid, null)
              if (reason != null && reason != None) {
                rcmdsForGuessCands.append(vid + ":video:bySortGs:" + reason)
              } else {
                rcmdsForGuessCands.append(vid + ":video:bySortGs")
              }
            }
          }
        }
      }
      if (uid.equals("135571358")) {
        println("guessInfo 4 135571358 meta:从离线结果中不分keyword直接取:rcmdsForGuessCands:" + rcmdsForGuessCands.mkString(","))
      }
    }*/
    (rcmdsForKeywordCands, rcmdsForGuessCands)
    // 补充

  }

  def isFilteredByAidOrKwd(resourcetype:String, entities:Seq[String], numThred:Int, recedResroucetypeAidOrKeywordsM: mutable.HashMap[String, Int]):Boolean = {

    var isFiltered = false
    for (entity <- entities if !isFiltered) {
      val resourcetypeAidOrKwd = resourcetype + "-" + entity
      val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
      if (cnt >= numThred)
        isFiltered = true
    }
    isFiltered
  }

  def addAidOrKwd(resourcetype:String, entities:Seq[String], recedResroucetypeAidOrKeywordsM: mutable.HashMap[String, Int]) = {

    for (entity <- entities) {
      val resourcetypeAidOrKwd = resourcetype + "-" + entity
      val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
      recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
    }
  }

  def getRecedKeyVideos(recedKeywords: String, recedKeyVideos: String, recedGuessVideos: String,
                        recedVideos: String,
                        recedAidOrKeywords: mutable.HashSet[String],
                        recedResources: mutable.HashSet[String],  recedVids: mutable.HashSet[String],
                        recedSids: mutable.HashSet[String], vidSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])],
                        ridSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])]) = {
    // /周杰伦:1362297:video:mArt,...  1362297:video:mArt&-1362297-v,
    //val uvs = recedKeyVideos.split("\t")
    if (recedKeywords != null && !recedKeywords.isEmpty) {
      for (kw <- recedKeywords.split(",")) {
        recedAidOrKeywords.add(kw)
      }
    }
    if (recedKeyVideos != null) {
      for (keyVideostr <- recedKeyVideos.split(",")) {
        val keyVideos = keyVideostr.split(":")
        // 已推荐kw
        recedAidOrKeywords.add(keyVideos(0))
        if (keyVideos.length >= 3) {
          if (keyVideos(2).equals("video")) {
            // kw已推荐videos
            recedVids.add(keyVideos(1))
            // 获取vid关联的sids、aids
            val (sids, aids) = vidSidAidM.getOrElse(keyVideos(1), (null, null))
            if (sids != null) {
              for (sid <- sids) {
                recedSids.add(sid)
              }
            }
            if (aids != null) {
              for (aid <- aids) {
                if (!aid.isEmpty) {
                  recedAidOrKeywords.add(aid)
                }
              }
            }
          } else {
            val ridType = keyVideos(1) + "-" + keyVideos(2)
            recedResources.add(ridType)
            val (sids, aids) = ridSidAidM.getOrElse(ridType, (null, null))
            if (sids != null) {
              for (sid <- sids) {
                recedSids.add(sid)
              }
            }
            if (aids != null) {
              for (aid <- aids) {
                if (!aid.isEmpty) {
                  recedAidOrKeywords.add(aid)
                }
              }
            }
          }
        }
      }
    }
    if (recedGuessVideos != null) {
      for (vs <- recedGuessVideos.split(",")) {
        val idtypereason = vs.split(":")
        if (idtypereason.size >= 2) { // 猜你喜欢, 不考虑关键词
          // 1362297:video:mArt
          if (idtypereason(1).equals("video")) {
            recedVids.add(idtypereason(0))
          } else {
            val ridType = idtypereason(0) + "-" + idtypereason(1)
            recedResources.add(ridType)
          }
        }
      }
    }
    if (recedVideos != null) {
      for (vid <- recedVideos.split(",")) {
        recedVids.add(vid)
      }
    }
  }

  def getRecedKeyVideosFromLog(recedKeywords: String, impressedKwdVideos: String, impressedGuessVideos: String,
                               recedVideos: String, recedAidOrKeywords: mutable.HashSet[String], recedResources: mutable.HashSet[String],  recedVids: mutable.HashSet[String], recedSids: mutable.HashSet[String],
                               vidSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])],
                               ridSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])]) = {

    try {
      if (recedKeywords != null && !recedKeywords.isEmpty) {
        for (kw <- recedKeywords.split(",")) {
          recedAidOrKeywords.add(kw)
        }
      }
      if (impressedKwdVideos != null) {
        for (idTypestr <- impressedKwdVideos.split(",")) {
          // idtype : lastRecedFromNow , idtype : lastRecedFromNow , ...
          val idTypeDay = idTypestr.split("-|:")
          if (idTypeDay.length >= 3) {
            if (idTypeDay(1).equals("video")) {
              // kw已曝光videos
              recedVids.add(idTypeDay(0))
              if (idTypeDay(2).toInt <= SONGART_REC_WINDOW) {
                // 获取vid关联的sids、aids
                val (sids, aids) = vidSidAidM.getOrElse(idTypeDay(0), (null, null))
                if (sids != null) {
                  for (sid <- sids) {
                    recedSids.add(sid)
                  }
                }
                if (aids != null) {
                  for (aid <- aids) {
                    if (!aid.isEmpty) {
                      recedAidOrKeywords.add(aid)
                    }
                  }
                }
              }
            } else {
              val ridType = idTypeDay(0) + "-" + idTypeDay(1)
              recedResources.add(ridType)
              if (idTypeDay(2).toInt <= 7) {
                val (sids, aids) = ridSidAidM.getOrElse(ridType, (null, null))
                if (sids != null) {
                  for (sid <- sids) {
                    recedSids.add(sid)
                  }
                }
                if (aids != null) {
                  for (aid <- aids) {
                    if (!aid.isEmpty) {
                      recedAidOrKeywords.add(aid)
                    }
                  }
                }
              }
            }
          }
        }
      }
      if (impressedGuessVideos != null) {
        for (idTypestr <- impressedGuessVideos.split(",")) {
          // idtype : lastRecedFromNow , idtype : lastRecedFromNow , ...
          val idTypeDay = idTypestr.split("-|:")
          if (idTypeDay.size >= 2) { // 猜你喜欢, 不考虑关键词
            if (idTypeDay(1).equals("video")) {
              recedVids.add(idTypeDay(0))
            } else {
              val ridType = idTypeDay(0) + "-" + idTypeDay(1)
              recedResources.add(ridType)
            }
          }
        }
      }
      if (recedVideos != null) {
        for (vid <- recedVideos.split(",")) {
          recedVids.add(vid)
        }
      }
    } catch {
      case ex:Exception => println("error in getRecedKeyVideosFromLog:" + ex.getMessage + "\nrecedKeywords:" + recedKeywords + "\nimpressedKwdVideos:" + impressedKwdVideos + "\nimpressedGuessVideos:" + impressedGuessVideos)
    }
  }


  def resortM(aidSidsM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    //val sortedAidSids = mutable.ArrayBuffer[(String, ArrayBuffer[String])]()
    val aidSidsArray = aidSidsM.toArray[(String, mutable.ArrayBuffer[String])]
    val sortedAidSids = aidSidsArray.sortWith(_._2.length > _._2.length)
    sortedAidSids
  }

  def appendAidSidsM(str: String, aidSidsM: mutable.HashMap[String, ArrayBuffer[String]], sidAidsM: mutable.HashMap[String, String]) = {
    if (str != null && !str.isEmpty) {
      // sid:...,sid
      for (sidx <- str.split(",")) {
        if (!sidx.isEmpty) {
          val sid = sidx.split(":")(0)
          val aidstr = sidAidsM.getOrElse(sid, null)
          if (aidstr != null) {
            val aids = aidstr.split(",")
            for (aid <- aids) {
              val sids = aidSidsM.getOrElse(aid, mutable.ArrayBuffer[String]())
              if (!sids.contains(sid)) {
                sids.append(sid)
                aidSidsM.put(aid, sids)
              }
            }
          }
        }
      }
    }
  }

  def appendAidsM(aidstr: String, aidSidsM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    if (aidstr != null) {
      for (aid <- aidstr.split(",")) {
        val sids = aidSidsM.getOrElse(aid, mutable.ArrayBuffer[String]())
        if (!sids.contains("0")) {
          sids.append("0") // 以0替代搜藏艺人
          aidSidsM.put(aid, sids)
        }
      }
    }
  }


  def getKeywords(keywordsInput: String) = {
    val keywords = mutable.HashSet[String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(keywordsInput)
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()

    while (line != null) {
      //keyword
      val keyword = line.trim
      if (!keyword.isEmpty) {
        keywords.add(keyword)
      }
      line = reader.readLine()
    }
    reader.close()
    keywords
  }

  def getVideoPredM(keywordsInput: String) = {
    val videoPredM = mutable.HashMap[String, Float]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(keywordsInput)
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      //video pred
      val ts = line.split("\t")
      if (ts.length >= 2) {
        videoPredM.put(ts(0), ts(1).toFloat)
      }
      line = reader.readLine()
    }
    reader.close()
    videoPredM
  }



  def getVideoInfoM(inputDir: String, sidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
                    aidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
                    videoPredM: mutable.HashMap[String, Float],
                    vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                    aidsidVidM: mutable.HashMap[String, String],
                    musicVideoS: Set[String]) = {
    val vidTitleM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getVideoInfoMFromFile(bufferedReader, vidTitleM, sidVideosM, aidVideosM, videoPredM, vidSidAidM, aidsidVidM, musicVideoS)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getVideoInfoMFromFile(bufferedReader, vidTitleM, sidVideosM, aidVideosM, videoPredM, vidSidAidM, aidsidVidM, musicVideoS)
      bufferedReader.close()
    }
    vidTitleM
  }

  def loadVideoTagIdNameM(inputDir: String) = {
    val vtagIdNameM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path)) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          loadVideoTagIdNameMFromFile(bufferedReader, vtagIdNameM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      loadVideoTagIdNameMFromFile(bufferedReader, vtagIdNameM)
      bufferedReader.close()
    }
    vtagIdNameM
  }

  def loadVideoTagIdNameMFromFile(bufferedReader: BufferedReader, vtagIdNameM: mutable.HashMap[String, String]) = {
    var line = bufferedReader.readLine
    while (line != null) {
      val info = line.split("\01")
      val tid = info(0)
      val tname = info(1)
      vtagIdNameM.put(tid, tname)

      line = bufferedReader.readLine
    }
  }

  def getFeatureResourceInfoM(inputDir:String,
                              vtagIdNameM: mutable.HashMap[String, String],
                              sidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              ridSidAidTidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                              aidsidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              provincenameCodeM: Map[String, String], ridPcodeM: mutable.HashMap[String, String]
                             ) = {
    val ridTitleM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path)) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getFeatureResourceInfoFromFile(bufferedReader, vtagIdNameM, ridTitleM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidTidM, aidsidResourcesM, provincenameCodeM, ridPcodeM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getFeatureResourceInfoFromFile(bufferedReader, vtagIdNameM, ridTitleM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidTidM, aidsidResourcesM, provincenameCodeM, ridPcodeM)
      bufferedReader.close()
    }
    ridTitleM
  }

  def getFeatureResourceInfoFromFile(reader: BufferedReader,
                                     vtagIdNameM: mutable.HashMap[String, String],
                                     ridTitleM: mutable.HashMap[String, String],
                                     sidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                                     kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                                     ridSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                                     aidsidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                                     provincenameCodeM: Map[String, String], ridPcodeM: mutable.HashMap[String, String]) = {
    var line = reader.readLine
    while (line != null) {
      try {
        implicit val formats = DefaultFormats
        val json = parse(line)
        val status = (json \ "status").extractOrElse[Int](-1)
        val forceRcmd = (json \ "forceRcmd").extractOrElse[Int](-1)
        val onlineTime = (json \ "onlineTime").extractOrElse[Long](0)
        val offlineTime = (json \ "offlineTime").extractOrElse[Long](0)
        val blocktype = parseBlocktype2resourcetype((json \ "blockType").extractOrElse[Int](-1))
        var resourcetype = ""
        var content = ""
        if (blocktype.equals("single")) {
          content = (json \ "content").extractOrElse[String]("null")
          if (!content.equals("null")) {
            val contentJson = parse(content)
            resourcetype = parse2type_feature((contentJson \ "resourceType").extractOrElse[Int](-1).toString)
          }
        }
        if (status==1 && forceRcmd == 0 && offlineTime>=NOW_TIME && onlineTime<=NOW_TIME && (usefulCombotypeS(resourcetype) || usefulSingletypeS.contains(resourcetype))) {
          val rid = (json \ "id").extractOrElse[Long](-1)
          val songIds = parseIds((json \ "songIds").extractOrElse[String](""))
          val artistIds = parseIds((json \ "artistIds").extractOrElse[String](""))
          val auditTagIds = parseIds((json \ "auditTagIds").extractOrElse[String](""))
          var title = (json \ "title").extractOrElse[String]("")
          title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
          val regions = (json \ "regions").extractOrElse[String]("")
          var provinceCode = "-1"
          if (regions != null && !regions.isEmpty) {
            val provinceName = (parse(regions) \ "province").extractOrElse[String]("null")
            provinceCode = parse2ProvinceCode(provinceName, provincenameCodeM)
          }

          println(s"feature remain: status:${status},forceRcmd:${forceRcmd},onlineTime:${onlineTime},offlineTime:${offlineTime},blocktype:${blocktype},content:${content},resourcetype:${resourcetype},rid:${rid},songIds:${songIds},artistIds:${artistIds},auditTagIds:${auditTagIds},title:${title},regions:${regions},provinceCode:${provinceCode}")
          if (!resourcetype.isEmpty) {
            val ridType = rid + "-" + resourcetype
            ridTitleM.put(ridType, title)
            if (!provinceCode.equals("-1"))
              ridPcodeM.put(ridType, provinceCode)

            if (songIds != null) {
              for (sid <- songIds) {
                if (!sid.isEmpty) {
                  val resources = sidResourcesM.getOrElse(sid, mutable.ArrayBuffer[String]())
                  sidResourcesM.put(sid, resources)
                  resources.append(ridType)

                  val sidAidPair = ridSidAidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidM.put(ridType, sidAidPair)
                  sidAidPair._1.append(sid)
                }
              }
            }

            if (artistIds != null) {
              for (aid <- artistIds) {
                if (!aid.isEmpty) {
                  val resources = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
                  aidResourcesM.put(aid, resources)
                  resources.append(ridType)

                  val sidAidPair = ridSidAidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidM.put(ridType, sidAidPair)
                  sidAidPair._2.append(aid)
                }
              }
            }

            if (auditTagIds != null) {
              for (tid <- auditTagIds) {
                if (!tid.isEmpty) {
                  val kwd = vtagIdNameM.getOrElse[String](tid, "")
                  if (!kwd.isEmpty) {

                    val resources = kwdResourceM.getOrElse(kwd, mutable.ArrayBuffer[String]())
                    kwdResourceM.put(kwd, resources)
                    resources.append(ridType)

                    val kwds = resourceKwdM.getOrElse(ridType, mutable.ArrayBuffer[String]())
                    resourceKwdM.put(ridType, kwds)
                    kwds.append(kwd)
                  }
                }
              }
            }

            if (songIds != null && artistIds != null) {
              for (sid <- songIds; aid <- artistIds) {
                val aidsid = aid + ":" + sid
                val ridTypes = aidsidResourcesM.getOrElse(aidsid, ArrayBuffer[String]())
                aidsidResourcesM.put(aidsid, ridTypes)
                if (!ridTypes.contains(ridType))
                  ridTypes.append(ridType)
              }
            }
          }

        } else
          println(s"feature filter: status:${status},forceRcmd:${forceRcmd},onlineTime:${onlineTime},offlineTime:${offlineTime},blocktype:${blocktype},content:${content},resourcetype:${resourcetype}")
      } catch {
        case ex: Exception => println("exception" + ex + ",line=" + line)
      }

      line = reader.readLine
    }
  }

  def getResourceInfoM(inputDir:String,
                       vtagIdNameM: mutable.HashMap[String, String],
                       sidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       //                       tagResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceTagM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       ridSidAidTidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                       aidsidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       provincenameCodeM: Map[String, String], ridPcodeM: mutable.HashMap[String, String], newconcertS: mutable.Set[String]
                      ) = {
    val ridTitleM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path)) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getResourceInfoFromFile(bufferedReader, vtagIdNameM, ridTitleM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidTidM, aidsidResourcesM, provincenameCodeM, ridPcodeM, newconcertS)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getResourceInfoFromFile(bufferedReader, vtagIdNameM, ridTitleM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidTidM, aidsidResourcesM, provincenameCodeM, ridPcodeM, newconcertS)
      bufferedReader.close()
    }
    ridTitleM
  }

  def getProvincenameCodeM():Map[String, String] = {

    val reader = new BufferedReader(new InputStreamReader(getClass.getResourceAsStream("/provinceMappings")))
    var retM = Map[String, String]()
    var line = reader.readLine
    while (line != null) {

      val info = line.split("\t")
      if (info.length >= 2) {
        retM += (info(0) -> info(1))
      }
      line = reader.readLine
    }
    reader.close
    retM
  }

  def getResourceInfoFromFile(reader: BufferedReader,
                              vtagIdNameM: mutable.HashMap[String, String],
                              ridTitleM: mutable.HashMap[String, String],
                              sidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              //                              tagResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceTagM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              ridSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                              aidsidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              provincenameCodeM: Map[String, String], ridPcodeM: mutable.HashMap[String, String],
                              newconcertS: mutable.Set[String]) = {
    var line = reader.readLine
    while (line != null) {
      try {
        implicit val formats = DefaultFormats
        val json = parse(line)
        val resourceId = (json \ "resourceId").extractOrElse[String]("")
        val resourceType = parse2type((json \ "resourceType").extractOrElse[String](""))
        if (usefulResourcetypeS.contains(resourceType)) {   // 只保留需要推荐的resources
          val songIds = parseIds((json \ "songIds").extractOrElse[String](""))
          val artistIds = parseIds((json \ "artistIds").extractOrElse[String](""))
          val auditTagIds = parseIds((json \ "auditTagIds").extractOrElse[String](""))
          var title = (json \ "title").extractOrElse[String]("综合推荐")
          title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
          val regions = (json \ "regions").extractOrElse[String]("")
          var provinceCode = "-1"
          if (regions != null && !regions.isEmpty) {
            val provinceName = (parse(regions) \ "province").extractOrElse[String]("null")
            provinceCode = parse2ProvinceCode(provinceName, provincenameCodeM)
          }
          if (resourceType.equals("concert") && artistIds.size == 1) {
            val createTime = (json \ "createTime").extractOrElse[Long](0L)
            if ((NOW_TIME - createTime) < 30 * 24 * 3600 * 1000L)
              newconcertS += resourceId
          }


          if (!resourceType.isEmpty) {
            val ridType = resourceId + "-" + resourceType
            ridTitleM.put(ridType, title)
            if (!provinceCode.equals("-1"))
              ridPcodeM.put(ridType, provinceCode)

            if (songIds != null) {
              for (sid <- songIds) {
                if (!sid.isEmpty) {
                  val resources = sidResourcesM.getOrElse(sid, mutable.ArrayBuffer[String]())
                  sidResourcesM.put(sid, resources)
                  resources.append(ridType)

                  val sidAidPair = ridSidAidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidM.put(ridType, sidAidPair)
                  sidAidPair._1.append(sid)
                }
              }
            }

            if (artistIds != null) {
              for (aid <- artistIds) {
                if (!aid.isEmpty) {
                  val resources = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
                  aidResourcesM.put(aid, resources)
                  resources.append(ridType)

                  val sidAidPair = ridSidAidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidM.put(ridType, sidAidPair)
                  sidAidPair._2.append(aid)
                }
              }
            }

            if (auditTagIds != null) {
              for (tid <- auditTagIds) {
                if (!tid.isEmpty) {
                  val kwd = vtagIdNameM.getOrElse[String](tid, "")
                  if (!kwd.isEmpty) {

                    val resources = kwdResourceM.getOrElse(kwd, mutable.ArrayBuffer[String]())
                    kwdResourceM.put(kwd, resources)
                    resources.append(ridType)

                    val kwds = resourceKwdM.getOrElse(ridType, mutable.ArrayBuffer[String]())
                    resourceKwdM.put(ridType, kwds)
                    kwds.append(kwd)

                    /*// 全量tag相关信息
                  val resources4tag = tagResourceM.getOrElse(kwd, mutable.ArrayBuffer[String]())
                  tagResourceM.put(kwd, resources4tag)
                  resources4tag.append(ridType)

                  val tags = resourceTagM.getOrElse(ridType, mutable.ArrayBuffer[String]())
                  resourceTagM.put(ridType, tags)
                  tags.append(kwd)*/
                  }
                }
              }
            }

            if (songIds != null && artistIds != null) {
              for (sid <- songIds; aid <- artistIds) {
                val aidsid = aid + ":" + sid
                val ridTypes = aidsidResourcesM.getOrElse(aidsid, ArrayBuffer[String]())
                aidsidResourcesM.put(aidsid, ridTypes)
                if (!ridTypes.contains(ridType))
                  ridTypes.append(ridType)
              }
            }
          }
        }
      } catch {
        case ex: Exception => println("exception" + ex + ",line=" + line)
      }

      line = reader.readLine
    }
  }


  def isValid(coverSize: String) = {
    var valid = false
    if (coverSize != null) {
      val wh = coverSize.split("\\*")
      if (wh.length >= 2) {
        val rate = wh(0).toDouble/wh(1).toDouble
        if (wh(0).toInt < 540 || wh(1).toInt < 300 ||  rate < 1.7 || rate > 1.8) {
          valid = false
        } else {
          valid = true
        }
      }
    }
    valid
  }

  def getVideoInfoMFromFile(reader: BufferedReader, vidTitleM: mutable.HashMap[String, String],
                            sidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]], aidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]],
                            videoPredM: mutable.HashMap[String, Float],
                            vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                            aidsidVidM: mutable.HashMap[String, String],
                            musicVideoS: Set[String]) = {
    // var line: String = null
    val curtime: Long = new Date().getTime
    var line = reader.readLine()
    while (line != null) {
      //      try {
      implicit val formats = DefaultFormats
      val json = parse(line)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      val songIds = parseIds((json \ "bgmIds").extractOrElse[String](""))
      val artistIds = parseIds((json \ "artistIds").extractOrElse[String](""))
      val auditTagIds = parseIds((json \ "auditTagIds").extractOrElse[String](""))
      val category = (json \ "category").extractOrElse[String]("").replace(",", "_")
      var title = (json \ "title").extractOrElse[String]("推荐视频")
      title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
      // TODO 白名单， minFLow, 分辨率
      println("vid:" + videoId);
      var validV = true
      if (!musicVideoS.contains(videoId))
        validV = false

      if (auditTagIds.contains("37028") || artistIds.contains("6452")) // 周杰伦相关tag过滤
        validV = false

      if (validV == true && (json \ "extData") != null) {
        // "extData":"{\"updateCover\":null,\"smallFlow\":false,\"expose\":true,\"coverSize\":\"1920*1080\",\"videoSize\":\"1280*720\"}
        val extDataStr = (json \ "extData").extractOrElse[String](null)
        val status = (json \ "status").extractOrElse[Int](0)
        val expTime = (json \ "expireTime").extractOrElse[Long](0)
        //println("extDataStr:" + extDataStr)

        //val extData = parse(extDataStr.substring(8, extDataStr.length - 1)) // TODO 去掉JString(...)
        if (extDataStr != null) {
          val extData = parse(extDataStr)
          //println("extData:" + extData)
          // 过滤掉cover、videoSize不符合要求的视频
          //println("status:" + status)
          //println("curtime:" + curtime)
          if (1 == status && extData != null && expTime > curtime) {
            // println("extData:" + extData)
            val smallFlow = (extData \ "smallFlow").extractOrElse[Boolean](false)
            // println((extData \ "smallFlow").extractOrElse[Boolean](false))
            if (smallFlow == true) { // 小流量
              println("filt by smallFlow:" + smallFlow)
              validV = false
            } else {
              val coverSize = (extData \ "coverSize").extractOrElse[String]("")
              //println("coverSize:" + coverSize)
              validV = isValid(coverSize)
              if (validV == true) {
                val videoSize = (extData \ "videoSize").extractOrElse[String]("")
                validV = isValid(videoSize)
                if (validV == true) {
                  val coverWatermark = (extData \ "coverWatermark").extractOrElse[Boolean](true)  // 默认值为true，没有检测过的都默认有水印
                  validV = !coverWatermark
                  if (validV == false) {
                    println("filt by coverWatermark:" + coverWatermark)
                  }
                } else {
                  println("filt by videoSize:" + videoSize)
                }
              } else {
                println("filt by coverSize:" + coverSize)
              }
            }
          } else {
            validV = false
            //println("extData:" + extData)
            //println("curtime:" + curtime)
            println("status, extData, time = " + (1==status) + "," + (extData != null) + "," + (expTime > curtime))

          }
        } else {
          validV = false
        }
      }

      if (validV == true) {
        vidTitleM.put(videoId, title)
        /* var fsid = ""
         var faid = ""*/
        if (songIds != null) {
          for (sid <- songIds) {
            if (!sid.isEmpty) {
              val videoPrds = sidVideosM.getOrElse(sid, mutable.ArrayBuffer[(String, Float)]())
              sidVideosM.put(sid, videoPrds)
              videoPrds.append((videoId, videoPredM.getOrElse(videoId, 0F)))
              val sidAidPair = vidSidAidM.getOrElse(videoId, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
              vidSidAidM.put(videoId, sidAidPair)
              sidAidPair._1.append(sid)
              //fsid = sid
            }
          }
        }

        if (artistIds != null) {
          for (aid <- artistIds) {
            if (!aid.isEmpty) {
              var videosPrds = aidVideosM.getOrElse(aid, null)
              if (videosPrds == null) {
                videosPrds = mutable.ArrayBuffer[(String, Float)]()
                aidVideosM.put(aid, videosPrds)
              }
              videosPrds.append((videoId, videoPredM.getOrElse(videoId, 0F)))
              val sidAidPair = vidSidAidM.getOrElse(videoId, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
              vidSidAidM.put(videoId, sidAidPair)
              sidAidPair._2.append(aid)
              //faid = aid
            }
          }
        }
        /*if (!fsid.isEmpty && !faid.isEmpty) {
          aidsidVidM.put(faid + ":" + fsid, videoId)
        }*/
        if (songIds != null && artistIds != null) {
          for (sid <- songIds; aid <- artistIds) {
            aidsidVidM.put(aid + ":" + sid, videoId)
          }
        }
      } else {
        println("Invalid video:" + line)
      }
      //} catch {
      // case ex: Exception => println("exception" + ex + ", line=" + line)
      //}
      line = reader.readLine()
    }
    val sidkeys = sidVideosM.keys.toArray[String]
    for (sid <- sidkeys) {
      val vidPrds = sidVideosM.getOrElse(sid, null)
      sidVideosM.put(sid, vidPrds.sortWith(_._2 > _._2))
    }

    val aidkeys = aidVideosM.keys.toArray[String]
    for (aid <- aidkeys) {
      val vidPrds = aidVideosM.getOrElse(aid, null)
      aidVideosM.put(aid, vidPrds.sortWith(_._2 > _._2))
    }
  }


  def getKeywordVideosM(videoInput: String, keywords: mutable.HashSet[String], vidTitleM: mutable.HashMap[String, String],
                        videoPredM: mutable.HashMap[String, Float]) = {
    val keywordVideosM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]]()
    val videoKeywordsM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    // 全量tag相关
    val tagVideosM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]]()
    val videoTagsM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(videoInput)
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = null
    line = reader.readLine()
    while (line != null) {
      try {
        // 1000052  \t  video  \t  薛之谦_ET,演员_ET
        val ts = line.split("\t")
        if (ts.length >= 3) {
          val vid = ts(0)
          if (vidTitleM.contains(vid)) { // 限定有效视频
            val kwds = mutable.ArrayBuffer[String]()
            val tags = mutable.ArrayBuffer[String]()
            for (tagType <- ts(2).split(",")) {
              val tagtype = tagType.split("_")
              if (tagtype.length >= 2) {
                val tag = tagtype(0)
                if (tagtype(1).equals("TT") && keywords.contains(tag)) {
                  val videoPrds = keywordVideosM.getOrElse(tag, mutable.ArrayBuffer[(String, Float)]())
                  keywordVideosM.put(tag, videoPrds)
                  if (!videoPrds.contains(vid))
                    videoPrds.append((vid, videoPredM.getOrElse(vid, 0F)))

                  kwds.append(tag)
                  videoKeywordsM.put(vid, kwds)
                }
                // 获取所有以"ET", "AT", "WK", "PT", "TT"为标记的tag相关信息
                if (usefulTagtypeS.contains(tagtype(1))) {
                  val videoPrds = tagVideosM.getOrElse(tag, mutable.ArrayBuffer[(String, Float)]())
                  tagVideosM.put(tag, videoPrds)
                  if (!videoPrds.contains(vid))
                    videoPrds.append((vid, videoPredM.getOrElse(vid, 0F)))

                  tags.append(tag)
                  videoTagsM.put(vid, tags)
                }
              }
            }
          }
        }
      }catch {
        case ex: Exception => println("io exception" + ex + ", line=" + line)
      }
      line = reader.readLine()
    }
    reader.close()

    var keys = keywordVideosM.keySet.toArray[String]
    for (key <- keys) { // 从大到小排列
      keywordVideosM.put(key, keywordVideosM.getOrElse(key, null).sortWith(_._2 > _._2))
    }
    keys = tagVideosM.keySet.toArray[String]
    for (key <- keys) { // 从大到小排列
      tagVideosM.put(key, tagVideosM.getOrElse(key, null).sortWith(_._2 > _._2))
    }
    (keywordVideosM, videoKeywordsM, tagVideosM, videoTagsM)
  }

  def getSongArtInfoM(inputDir: String, sidAidsM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String],
                      aidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]], sidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]],
                      aidResourcesM: mutable.HashMap[String, ArrayBuffer[String]], sidResourcesM: mutable.HashMap[String, ArrayBuffer[String]]) = {

    // getSongArtInfoM(inputDir: String, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String])= {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getSongArtInfoMFromFile(bufferedReader, sidAidsM, aidNameM, aidVideosM, sidVideosM, aidResourcesM, sidResourcesM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSongArtInfoMFromFile(bufferedReader, sidAidsM, aidNameM,  aidVideosM, sidVideosM, aidResourcesM, sidResourcesM)
      bufferedReader.close()
    }
  }

  def getSongArtInfoMFromFile(reader: BufferedReader, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String],
                              aidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]], sidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]],
                              aidResourcesM: mutable.HashMap[String, ArrayBuffer[String]], sidResourcesM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    var line: String = reader.readLine()
    while (line != null) {
      // 59879	1873:阿宝	10846:张冬玲
      if (line != null) {
        val ts = line.split("\t", 2)
        var sid = ts(0)
        if (sid.startsWith("-")) {
          sid = sid.substring(1)
        }
        /*
        var sidLong = ts(0).toLong
        if (sidLong < 0) sidLong *= -1
        val sid = sidLong.toString
        */
        val aids = mutable.ArrayBuffer[String]()
        if (!sid.isEmpty) {
          if (ts.length >= 2) {
            for (aidnameStr <- ts(1).split("\t")) {
              val aidname = aidnameStr.split(":", 2)
              if (aidname.length >= 2) {
                val aid = aidname(0)
                if (!aid.isEmpty) {
                  aids.append(aid)
                  val aname = aidname(1)
                  if (!aidNameM.contains(aid) && !aname.isEmpty && (aidVideosM.contains(aid) || aidResourcesM.contains(aid))) { // video或者resource里面有这个艺人
                    aidNameM.put(aid, aname.replace(",", " "))
                  }
                }
              }
            }
          }
          if (aids.length > 0 && (sidVideosM.contains(sid) || sidResourcesM.contains(sid))) { // video或者resource里面有这首歌
            sidAidM.put(sid, aids.mkString(","))
          }
        }
      }
      line = reader.readLine()
    }
  }

  // def getmvArtsM(inputDir: String, aidNameM: mutable.HashMap[String, String])= {
  def getinfoRromMV(inputDir: String,
                    mvPoolS:mutable.Set[String],
                    ridSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])],
                    aidResourcesM: mutable.HashMap[String, ArrayBuffer[String]], sidResourcesM: mutable.HashMap[String, ArrayBuffer[String]],
                    aidsidResourcesM: mutable.HashMap[String, ArrayBuffer[String]]) = {

    val aidTagsM = new mutable.HashMap[String, mutable.HashMap[String, Int]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getmvArtsMFromFile(bufferedReader, mvPoolS, aidTagsM, ridSidAidM, aidResourcesM, aidResourcesM, aidsidResourcesM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getmvArtsMFromFile(bufferedReader, mvPoolS, aidTagsM, ridSidAidM, aidResourcesM, aidResourcesM, aidsidResourcesM)
      bufferedReader.close()
    }
    aidTagsM
  }

  def getmvArtsMFromFile(reader: BufferedReader,
                         mvPoolS: mutable.Set[String],
                         aidTagsM: mutable.HashMap[String, mutable.HashMap[String, Int]],
                         ridSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])],
                         aidResourcesM: mutable.HashMap[String, ArrayBuffer[String]], sidResourcesM: mutable.HashMap[String, ArrayBuffer[String]],
                         aidsidResourcesM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    var line: String = reader.readLine()

    while (line != null) {
      // ID,Name,Artists,Tags,MVDesc,
      // Valid,AuthId,Status,ArType,MVStype,
      // Subtitle,Caption,Area,Type,SubType,
      // Neteaseonly,Upban,Plays,Weekplays,Dayplays,
      // Monthplays,Mottos,Oneword,Appword,Stars,
      // Duration,Resolution,FileSize,Score,PubTime,
      // PublishTime,Online,ModifyTime,ModifyUser,TopWeeks,
      // SrcFrom,SrcUplod,AppTitle,Subscribe,transName,
      // aliaName,alias,fee
      // println("LINE:" + line)
      if (line != null) {
        val ts = line.split("\01")
        if (ts != null && ts.length >= 3) {
          val mvid = ts(0)
          if (mvPoolS.contains(mvid)) {
            if (ts.length >= 8 && !ts(7).isEmpty) {
              val status = ts(7).toInt
              if (status != -9) { // 过滤艺人赞赏mvid
                val mvidtype = mvid + "-mv"
                val artids = parseArts(ts(2))
                ridSidAidM.put(mvidtype, (mutable.ArrayBuffer[String](), artids))
                val arttags = mutable.ArrayBuffer[String]()
                if (ts.length >= 13) {
                  val area = parseTags(ts(12))
                  val mvtype = parseTags(ts(9))
                  val artype = parseTags(ts(8))
                  arttags.appendAll(comb(area, mvtype, artype))
                }
                //val anameBuf = new StringBuilder()
                for (aid <- artids) {
                  //val aname = aidNameM.getOrElse(aid, "")
                  //anameBuf.append(aid).append(":").append(aname).append(",")
                  if (!arttags.isEmpty) {
                    for (tag <- arttags) {
                      val tagCntM = aidTagsM.getOrElse(aid, mutable.HashMap[String, Int]())
                      aidTagsM.put(aid, tagCntM) // aid 及其对应的 tagCntM( tag -> tagCnt)
                      tagCntM.put(tag, tagCntM.getOrElse(tag, 0) + 1)
                    }
                  }
                  val ridtypes = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
                  aidResourcesM.put(aid, ridtypes)
                  ridtypes.append(mvidtype)
                }
              }
              else
                println("mvid:" + mvid + " filtered by zanshang mv rules...")
            }
          }
        }
      }
      line = reader.readLine()
    }
  }

  def parseIds(str: String) = {
    val res = mutable.ArrayBuffer[String]()
    if (!str.isEmpty && !str.equalsIgnoreCase("null")) {
      res.appendAll(str.substring(1, str.length-1).split(","))
    }
    res
  }
  def parseTags(strin: String) = {
    val str = strin.trim
    str.split(";")
  }
  def parse2type(str: String) = {
    if (str.equals("6")) {
      "topic"
    } else if (str.equals("21")) {
      "evtTopic"
    } else if (str.equals("22")) {
      "concert"
    } else
      ""
  }
  def parse2type_feature(str: String) = {
    if (str.equals("5")) {
      "mv_f"
    } else if (str.equals("6")) {
      "topic_f"
    } else if (str.equals("21")) {
      "evtTopic_f"
    } else if (str.equals("22")) {
      "concert_f"
    } else if (str.equals("62")) {
      "video_f"
    } else
      ""
  }
  def parseBlocktype2resourcetype(blocktype: Long) = {
    if (blocktype == 4) {
      "keyword_f"
    } else if (blocktype == 5) {
      "comboConcert_f"
    } else if (blocktype == 7) {
      "newMv_f"
    } else if (blocktype == 8) {
      "single"
    } else
      ""
  }
  def parseResourcetype2Blocktype(resourcetype: String) = {
    if (resourcetype.equals("keyword_f")) {
      4
    } else if (resourcetype.equals("comboConcert_f")) {
      5
    } else if (resourcetype.equals("newMv_f")) {
      7
    } else if (usefulSingletypeS.contains(resourcetype)) {
      8
    } else
      -1
  }
  def parse2ProvinceCode(provinceName: String, mapingsM: Map[String, String]) = {

    var provinceName_final = provinceName
    if (provinceName.contains("新疆"))
      provinceName_final = "新疆"
    else if (provinceName.contains("广西"))
      provinceName_final = "广西"
    else if (provinceName.contains("宁夏"))
      provinceName_final = "宁夏"
    else if (provinceName.contains("香港"))
      provinceName_final = "香港"
    else if (provinceName.contains("澳门"))
      provinceName_final = "澳门"
    else if (provinceName.contains("西藏"))
      provinceName_final = "西藏"

    if (mapingsM.contains(provinceName_final))
      mapingsM(provinceName_final)
    else
      "-1"
  }

  def parseArts(str: String): ArrayBuffer[String] = {
    val res = mutable.ArrayBuffer[String]()
    var artstr = str.trim
    if (!artstr.isEmpty && !artstr.equals("NULL")) {
      val arts = artstr.split("\t")
      for (art <- arts) {
        val artid = art.split(":")(0).trim
        if (!artid.equals("0")) {
          res.append(artid)
        }
      }
    }
    res
  }
  def comb(areas: Array[String], mvtypes: Array[String], artypes: Array[String]) = {
    val res = mutable.ArrayBuffer[String]()
    for (area <- areas) {
      for (mvtype <- mvtypes) {
        for (artype <- artypes) {
          var ama = area + mvtype + artype
          ama = ama.replaceAll("null", "")
          if (!ama.isEmpty) {
            res.append(ama)
          }
        }
      }
    }
    res
  }

  def flushTopRcmdData(keywordVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]], outKeywords: String, outKeywordVideo: String, vidTitleM: mutable.HashMap[String, String]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())

    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(outKeywords))))
    bwriter.write("TAGS\t" + keywordVideosM.keys.mkString(",") + "\n")
    bwriter.close()

    bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(outKeywordVideo))))
    for ((keyword, videoPrds) <- keywordVideosM) {
      val top1000 = mutable.ArrayBuffer[String]()
      val cnt = 0
      // while (top1000.size < 1000 && top1000.size < videoPrds.length) {
      for ((vid, prd) <- videoPrds) {
        // top1000.append(keyword + ":" + videoPrds(top1000.size)._1 + ":video:hotkey:热门推荐")
        //if (vidTitleM.contains(videoPrds(top1000.size)._1)) { // 过滤
        //  top1000.append(videoPrds(top1000.size)._1)
        //}
        if (vidTitleM.contains(vid)) {
          top1000.append(vid)
        }
      }
      bwriter.write(keyword + "\t" + top1000.mkString(",") + "\n")
    }
    bwriter.close()
  }

  def flushVideoTitle(vidTitleM: mutable.HashMap[String, String], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((vid, title) <- vidTitleM) {
      bwriter.write(vid + ":video\t" + title + "\n")
    }
    bwriter.close()
  }

  def flushHotVideo(vidTitleM: mutable.HashMap[String, String], hotVideos: Array[Row], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    val validVtypes = mutable.ArrayBuffer[String]()
    var vkey = "video"
    // val line = hotVideos.apply(0).toString()
    for (line <- hotVideos) {
      val kv = line.get(0).toString().split("\t")
      if (kv.length >= 2) {
        vkey = kv(0)
        for (vidtype <- kv(1).split(",")) {
          val vidType = vidtype.split(":")
          if (vidType(1).equals("video") && vidTitleM.contains(vidType(0))) {
            validVtypes.append(vidtype)
          }
          /*
          else {
            validVtypes.append(vidtype)
          }*/
        }
      }
    }

    bwriter.write(vkey + "\t" + validVtypes.mkString(",") + "\n")

    bwriter.close()
  }


  def getVideoDupM(input: String, vidPredM: mutable.HashMap[String, Float]) = {
    val vid2ShowvidM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(input)

    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // id \t id \t id
      val ids = line.split("\t")
      var maxPredId = ids(0)
      var maxPred = vidPredM.getOrElse(maxPredId, 0F)
      for (i <- 1 until ids.length) {
        val curPred = vidPredM.getOrElse(ids(i), 0F)
        if (curPred > maxPred) {
          maxPred = curPred
          maxPredId = ids(i)
        }
      }
      val idset = mutable.HashSet[String]()
      for (id <- ids) {
        if (!id.equals(maxPredId)) {
          vid2ShowvidM.put(id, maxPredId)
        }
      }
      line = reader.readLine()
    }
    reader.close()
    vid2ShowvidM
  }

  def trim(title: String, stopwords: mutable.HashSet[Char]) = {
    val sb = new mutable.StringBuilder()
    for (c <- title.toCharArray) {
      if (!stopwords.contains(c)) {
        sb.append(c)
      }
    }
    sb.toString
  }

  def removeInvalid(vidTitleM: mutable.HashMap[String, String], vid2TopvidM: mutable.HashMap[String, String], stopwords: mutable.HashSet[Char], vidPredM: mutable.HashMap[String, Float]) = {
    val rmSet = mutable.HashSet[String]()
    val trimTitlesM = mutable.HashMap[String, String]()
    for ((vid, title) <- vidTitleM) {
      if (vid2TopvidM.contains(vid) && vidTitleM.contains(vid2TopvidM(vid))) { // 有重复，且映射的最终vid有效，删除原vid
        rmSet.add(vid)
        println("dup video:" + vid2TopvidM(vid) + "-vs-" + vid)
      }
      val trimTitle = trim(title, stopwords)
      if (!trimTitlesM.contains(trimTitle)) {
        trimTitlesM.put(trimTitle, vid)
      } else {
        println("dup title:" + vid + " -vs- " + trimTitlesM(trimTitle) + "->" + trimTitle)
        val prd1 = vidPredM.getOrElse(vid, 0F)
        val prd2 = vidPredM.getOrElse(trimTitlesM(trimTitle), 0F)
        if (prd1 <= prd2) {
          rmSet.add(vid)
          println("rm:" + vid)
        } else {
          rmSet.add(trimTitlesM(trimTitle))
          trimTitlesM.put(trimTitle, vid)
          println("rm:" + trimTitlesM(trimTitle))
        }
      }
    }
    println("rmSet size:" + rmSet.size)
    for (rmid <- rmSet) {
      vidTitleM.remove(rmid)
    }
  }


  def getStopwords(input: String) = {
    val stopwords = new mutable.HashSet[Char]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(input)

    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // word
      if (!line.isEmpty) {
        stopwords.add(line.charAt(0))
      }
      line = reader.readLine()
    }
    reader.close()
    stopwords
  }


  def getArtTagsM(artTagsInput: String) = {
    val uidTagsM = new mutable.HashMap[String, Seq[String]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(artTagsInput)

    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // aid \t name \t tag,tag
      if (!line.isEmpty) {
        val ts = line.split("\t", 3)
        if (ts.length >= 3) {
          uidTagsM.put(ts(0), ts(2).split(","))
        }
      }
      line = reader.readLine()
    }
    reader.close()
    uidTagsM
  }

  def getArtSimsM(pathInput: Path, artSimsM:  mutable.HashMap[String, mutable.LinkedHashSet[String]]) = {

    val hdfs: FileSystem = FileSystem.get(new Configuration())
    // val path = new Path(artSimsInput)
    if (hdfs.isDirectory(pathInput)) {
      for (status <- hdfs.listStatus(pathInput) ) {
        val fpath = status.getPath
        getArtSimsMF(fpath, artSimsM)
      }
    } else {
      getArtSimsMF(pathInput, artSimsM)
      //uidSimsM
    }

  }


  def getArtSimsMF(fpath: Path, artSimsM: mutable.HashMap[String, mutable.LinkedHashSet[String]]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
    var line: String = reader.readLine()
    while (line != null) {
      // aid \t aid:score,aid:score
      if (!line.isEmpty) {
        val ts = line.split("\t", 2)
        if (ts.length >= 2) {
          val idScores = ts(1).split(",")
          val simaids = mutable.LinkedHashSet[String]()
          for (ids <- idScores) {
            val idScore = ids.split(":")
            simaids.add(idScore(0))
          }
          artSimsM.put(ts(0), simaids)
        }
      }
      line = reader.readLine()
    }
    reader.close()
  }



  def getKwordSimaids(aid: String
                      ,aidSidsM: mutable.HashMap[String, ArrayBuffer[String]]
                      ,artSimsM: mutable.HashMap[String, mutable.LinkedHashSet[String]]
                      ,artTagsM: mutable.HashMap[String, Seq[String]]
                      ,kwdTagM: Map[String, String]
                      ,recedAidOrKeyword: mutable.HashSet[String]) = {
    val res = mutable.ArrayBuffer[String]()
    res.append(aid)
    //println("aid:" + aid)
    val tags = artTagsM.getOrElse(aid, null)
    //println("tags:" + tags)
    val tagSaidScoreM = mutable.HashMap[String, mutable.LinkedHashMap[String, Int]]()
    if (tags != null) {
      val sims = artSimsM.getOrElse(aid, null)
      //println("sims:" + sims)
      if (sims != null) {
        for (said <- sims) {
          val stags = artTagsM.getOrElse(said, null)
          //println("stags:" + stags)
          if (stags != null) {
            //for (stag <- stags) {
            var stag = stags(0) // 优先用mv tag
            if (tags.contains(stag)) { // 有相同tag
              //println("Match:" + stag)
              val saidScoreM = tagSaidScoreM.getOrElse(stag, mutable.LinkedHashMap[String, Int]()) // 保持有先后顺序
              tagSaidScoreM.put(stag, saidScoreM)
              saidScoreM.put(said, saidScoreM.getOrElse(said, 0) + 1)
              if (aidSidsM.contains(said)) {  // 命中根据songid推测的用户artist偏好
                saidScoreM.put(said, saidScoreM.getOrElse(said, 0) + 1)
              }
            }
            //}
          }
        }
      }
      var maxTag = ""
      var maxSaidNum = 0;
      for ((tag, saidScoreM) <- tagSaidScoreM) { // 选择行为最多，相似度最高的
        if (saidScoreM.size > maxSaidNum) {
          maxTag = tag
          maxSaidNum = saidScoreM.size
        }
      }
      var maxScore = 1
      if (!maxTag.isEmpty) {
        for ((said, score) <- tagSaidScoreM(maxTag).toArray[(String, Int)].sortWith(_._2 > _._2)) {
          //println(said + "->" + score)
          res.append(said)
          if (score > maxScore) {
            maxScore = score
          }
        }

      }
      //println("res:" + res)
      if (kwdTagM.contains(maxTag))
        maxTag = kwdTagM(maxTag)
      (maxTag, res, maxScore)
    } else {
      ("", null, 1)
    }
  }

  def resourcetypeNeed(resourcetypeResrouceM: mutable.Map[String, ArrayBuffer[String]], resourcetype: String, num4resource: Int): Boolean = {

    var needed = false
    val buffer = resourcetypeResrouceM.getOrElse(resourcetype, ArrayBuffer[String]())
    if (buffer.size < num4resource)
      needed = true

    needed
  }

  def resourcesNeed(resourcetypeResrouceM: mutable.Map[String, ArrayBuffer[String]], num4resource: Int): Boolean = {

    var needed = false
    if (resourcetypeResrouceM.size < (usefulResourcetypeS.size + usefulCombotypeS.size + usefulSingletypeS.size)) {
      needed = true
    } else {
      for (buffer <- resourcetypeResrouceM.values if !needed) {
        if (buffer.size < num4resource)
          needed = true
      }
    }
    needed
  }

  def getRcmd(aid: String, keyword: String, sids: mutable.ArrayBuffer[String],
              rcmdsForArtCands: mutable.ArrayBuffer[String], resourcetypeResrouceM: mutable.Map[String, ArrayBuffer[String]],
              recedSids: mutable.HashSet[String], recedResources: mutable.HashSet[String], recedVids: mutable.HashSet[String], recedResroucetypeAidOrKeywordsM: mutable.HashMap[String, Int],
              kwdResourcetypesetM: mutable.HashMap[String, mutable.HashSet[String]], // 每个关键词包含的resourceSet
              vidTitleM: mutable.HashMap[String, String],
              vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
              ridSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
              aidsidResourcesM: mutable.HashMap[String, ArrayBuffer[String]], aidsidVideosM: mutable.HashMap[String, String],
              aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
              ridPcodeM: mutable.HashMap[String ,String],
              musicVidS: Set[String], newmvS: mutable.Set[String], newconcertS: mutable.Set[String],
              num4kwd: Int, partAlg: String,
              uidPcode: String) {

    var kwdFinal = keyword.replaceAll(":", "")
    if (partAlg.equals("mart"))
      kwdFinal = kwdFinal + "-" + aid
    val random = new Random()
    val sidIter = sids.iterator
    //var resourcesMatchesCnt = 0
    var needAidsidMatch4kwd = true
    val kwdContainResourcetypeS = kwdResourcetypesetM.getOrElse(kwdFinal, mutable.HashSet[String]())
    if (kwdContainResourcetypeS.isEmpty)
      kwdResourcetypesetM.put(kwdFinal, kwdContainResourcetypeS)
    while (sidIter.hasNext &&
      (rcmdsForArtCands.size < num4kwd || resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE))
    ){
      val sid = sidIter.next
      if (!recedSids.contains(sid)) { // 歌曲没有推荐过
        val aidsid = aid + ":" + sid
        val ridTypes = aidsidResourcesM.getOrElse(aidsid, ArrayBuffer[String]())
        for (ridType <- ridTypes if (needAidsidMatch4kwd || resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE))) {
          // aid+sid同时命中user偏好的resources
          if (!recedResources.contains(ridType)) {
            var ridPcode = "-1"
            if (ridPcodeM.contains(ridType))
              ridPcode = ridPcodeM(ridType)
            val rid = ridType.split("-")(0)
            var resourcetype = ridType.split("-")(1)
            // 关键词资源多样性
            if (!kwdContainResourcetypeS.contains(resourcetype)) {
              // 演唱会同城过滤
              if (!resourcetype.equals("concert") || (resourcetype.equals("concert") && ridPcode.equals(uidPcode))) {

                val resourcetypeAidOrKwd = resourcetype + "-" + aid
                val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
                if (cnt < GUESS_RESOURCE_THRED) {
                  val resourceRecallA = resourcetypeResrouceM.getOrElse(resourcetype, ArrayBuffer[String]())
                  resourcetypeResrouceM += (resourcetype -> resourceRecallA)

                  if (needAidsidMatch4kwd) {
                    // 关键词逻辑
                    rcmdsForArtCands.append(kwdFinal + ":" + ridType.replaceAll("-", ":") + ":" + partAlg + "song:" + sid + "-song")
                    needAidsidMatch4kwd = false

                    recedResources.add(ridType)
                    kwdContainResourcetypeS.add(resourcetype)
                    recedSids.add(sid)
                    recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)

                  } else if (resourcetypeNeed(resourcetypeResrouceM, resourcetype, CAND_MAX_NUM_FOR_SINGLE_RESOURCE)) {
                    // 综合资源单独

                    // 区分新mv和新concert
                    if (resourcetype.equals("mv") && newmvS.contains(rid))
                      resourcetype = "newMv"
                    else if (resourcetype.equals("concert") && newconcertS.contains(rid))
                      resourcetype = "newConcert"

                    if (resourcetype.startsWith("new"))
                      resourceRecallA.insert(0, rid + ":" + resourcetype + ":" + partAlg + "song:" + sid + "-song&" + aid + "-artist")
                    else
                      resourceRecallA.append(rid + ":" + resourcetype + ":" + partAlg + "song:" + sid + "-song")

                    recedResources.add(ridType)
                    kwdContainResourcetypeS.add(resourcetype)
                    recedSids.add(sid)
                    recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
                  }
                }
              }
            }
          }
        }
        // aid命中user偏好的resrouces
        // TODO 命中aid的user pref更精准些，所以暂时不对aid维度限制ridType的数量(外层调用已经做了)
        if (rcmdsForArtCands.size < num4kwd || resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE)) {

          val resources = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
          if (!resources.isEmpty) {
            var startIdx = random.nextInt(resources.length) // 随机化
            while (startIdx < resources.length && (needAidsidMatch4kwd || resourcesNeed(resourcetypeResrouceM, CAND_MAX_NUM_FOR_SINGLE_RESOURCE))) {
              val ridType = resources(startIdx)
              if (!recedResources.contains(ridType)) {
                var ridPcode = "-1"
                if (ridPcodeM.contains(ridType))
                  ridPcode = ridPcodeM(ridType)
                val rid = ridType.split("-")(0)
                var resourcetype = ridType.split("-")(1)

                // 关键词资源多样性
                if (!kwdContainResourcetypeS.contains(resourcetype)) {
                  val resourcetypeAidOrKwd = resourcetype + "-" + aid
                  val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
                  if (cnt < GUESS_RESOURCE_THRED) {
                    // 演唱会同城过滤
                    if (!ridType.endsWith("concert") || (ridType.endsWith("concert") && ridPcode.equals(uidPcode))) {
                      if (needAidsidMatch4kwd && rcmdsForArtCands.size < num4kwd) {
                        // 关键词相关
                        rcmdsForArtCands.append(kwdFinal + ":" + ridType.replaceAll("-", ":") + ":" + partAlg + ":" + aid + "-artist")

                        recedResources.add(ridType)
                        kwdContainResourcetypeS.add(resourcetype)
                        recedSids.add(sid)
                        recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
                        //resourcesMatchesCnt += 1
                      } else if (resourcetypeNeed(resourcetypeResrouceM, resourcetype, CAND_MAX_NUM_FOR_SINGLE_RESOURCE)) {
                        // 综合资源单独
                        val resourcetypeAidOrKwd = resourcetype + "-" + aid
                        val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
                        if (cnt < GUESS_RESOURCE_THRED) {
                          val resourceRecallA = resourcetypeResrouceM.getOrElse(resourcetype, ArrayBuffer[String]())
                          resourcetypeResrouceM += (resourcetype -> resourceRecallA)

                          // 区分新mv和新concert
                          if (resourcetype.equals("mv") && newmvS.contains(rid))
                            resourcetype = "newMv"
                          else if (resourcetype.equals("concert") && newconcertS.contains(rid))
                            resourcetype = "newConcert"

                          if (resourcetype.startsWith("new"))
                            resourceRecallA.insert(0, rid + ":" + resourcetype + ":" + partAlg + ":" + aid + "-artist")
                          else
                            resourceRecallA.append(rid + ":" + resourcetype + ":" + partAlg + ":" + aid + "-artist")

                          recedResources.add(ridType)
                          kwdContainResourcetypeS.add(resourcetype)
                          recedSids.add(sid)
                          recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
                        }
                      }
                    }
                  }
                }
              }
              startIdx += 1
            }
          }
        }
        // aid+sid同时命中user偏好的videos
        if (rcmdsForArtCands.size < num4kwd) {
          val aidsid = aid + ":" + sid
          val vid = aidsidVideosM.getOrElse(aidsid, null)
          if (vid != null && vidTitleM.contains(vid) && !recedVids.contains(vid)) {
            var remained = true
            if (partAlg.equals("mtag") && !musicVidS.contains(vid))
              remained = false
            if (remained) {

              val resourcetype = "video"
              val resourcetypeAidOrKwd = resourcetype + "-" + aid
              val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
              if (cnt < GUESS_RESOURCE_THRED) {
                rcmdsForArtCands.append(kwdFinal + ":" + vid + ":video:" + partAlg + "song:" + sid + "-song")
                recedVids.add(vid)
                recedSids.add(sid)
                recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
              }
            }
          }
        }

      }
    }
    // aid命中user偏好的videos
    val videoPrds = aidVideosM.getOrElse(aid, null)
    if (videoPrds != null) {
      val vIter = videoPrds.iterator
      while (vIter.hasNext && rcmdsForArtCands.length < num4kwd) {
        val vidPrd = vIter.next
        val vid = vidPrd._1
        val (sids, aids) = vidSidAidM.getOrElse(vid, (null, null))
        if (!recedVids.contains(vid) &&
          vidTitleM.contains(vid) &&
          !isSongReced(sids, recedSids)
        ) {
          var remained = true
          if (partAlg.equals("mtag") && !musicVidS.contains(vid))
            remained = false
          if (remained) {

            val resourcetype = "video"
            val resourcetypeAidOrKwd = resourcetype + "-" + aid
            val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
            if (cnt < GUESS_RESOURCE_THRED) {
              rcmdsForArtCands.append(kwdFinal + ":" + vid + ":video:" + partAlg + ":" + aid + "-artist")
              recedVids.add(vid)
              addRecedSong(sids, recedSids)
              recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
            }
          }
        }
      }
    }
    // 不够的情况下继续补充resource
    // aid命中user偏好的resources
    if (rcmdsForArtCands.size < num4kwd) { // 视频不够，mv补充
      val resources = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
      val resourceIter = resources.iterator
      while (resourceIter.hasNext && rcmdsForArtCands.size < num4kwd) {
        val ridType = resourceIter.next
        val (sids, aids) = ridSidAidM.getOrElse(ridType, (null, null))
        if (!recedResources.contains(ridType) &&
          !isSongReced(sids, recedSids) &&
          rcmdsForArtCands.size < num4kwd
        ) {
          var ridPcode = "-1"
          if (ridPcodeM.contains(ridType))
            ridPcode = ridPcodeM(ridType)
          // 演唱会同城过滤
          if (!ridType.endsWith("concert") || (ridType.endsWith("concert") && ridPcode.equals(uidPcode))) {

            val resourcetype = ridType.split("-")(1)
            // 关键词资源多样性
            if (!kwdContainResourcetypeS.contains(resourcetype)) {
              val resourcetypeAidOrKwd = resourcetype + "-" + aid
              val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
              if (cnt < GUESS_RESOURCE_THRED) {
                rcmdsForArtCands.append(kwdFinal + ":" + ridType.replaceAll("-", ":") + ":" + partAlg + ":" + aid + "-artist")
                recedResources.add(ridType)
                kwdContainResourcetypeS.add(resourcetype)
                addRecedSong(sids, recedSids)
                recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
              }
            }
          }
        }
      }
    }
  }

  def isMusicVideo(musicVideoCategoriesSet: mutable.Set[String], eventCategory: String):Boolean = {

    val category1 = eventCategory.split("_")(0) + "_"
    if (musicVideoCategoriesSet.contains(category1) || musicVideoCategoriesSet.contains(eventCategory))
      true
    else
      false
  }
}
