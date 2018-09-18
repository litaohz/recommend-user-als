package com.netease.music.recommend.scala.feedflow

import java.io.{BufferedReader, InputStreamReader}

import com.netease.music.recommend.scala.feedflow.cf.setConcat
import com.netease.music.recommend.scala.feedflow.mainpage.GetMainpageRcmdResource.{loadVideoTagIdNameM, parse2type, parseIds, usefulResourcetypeS}
import com.netease.music.recommend.scala.feedflow.mainpage.RecVideoByAidSid.{flushStringArrayBufferArrayBufferArrayBufferM, flushStringArrayBufferM, flushStringStringM}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

/**
  * Created by hzzhangjunfei1 on 2017/8/1.
  */
object GetMvPool_official {

  val NOW_TIME = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)
    import spark.implicits._

    val options = new Options
    options.addOption("Music_MVMeta_allfield", true, "input directory")
    options.addOption("videoMvTag", true, "input directory")
    options.addOption("videoMvInfo", true, "input directory")
    options.addOption("mvSongPair", true, "input directory")
    options.addOption("videoGroupMvTagRelation", true, "input directory")
    options.addOption("controversialFigures", true, "input directory")
    options.addOption("vieoScore", true, "input directory")
    options.addOption("videoClickrate", true, "input directory")
    options.addOption("Music_VideoTag", true, "Music_VideoTag")
    options.addOption("Music_RcmdResource", true, "Music_RcmdResource")

    options.addOption("output", true, "output directory")
    options.addOption("outputPath4nkv", true, "outputPath 4 redis directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val Music_MVMeta_allfield = cmd.getOptionValue("Music_MVMeta_allfield")
    val videoMvTag = cmd.getOptionValue("videoMvTag")
    val videoMvInfo = cmd.getOptionValue("videoMvInfo")
    val mvSongPair = cmd.getOptionValue("mvSongPair")
    val videoGroupMvTagRelation = cmd.getOptionValue("videoGroupMvTagRelation")
    val controversialFigures = cmd.getOptionValue("controversialFigures")
    val vieoScore = cmd.getOptionValue("vieoScore")
    val videoClickrate = cmd.getOptionValue("videoClickrate")

    val outputPath = cmd.getOptionValue("output")
    val outputPath4nkv = cmd.getOptionValue("outputPath4nkv")

    // 争议艺人集合
    val controversialArtistIdSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(controversialFigures)
      .collect()
      .foreach(line => {
        val info = line.split("\t")
        val controversialArtistid = info(0)
        controversialArtistIdSet += controversialArtistid
      })
    println("controversialArtistIdSet:\t" + controversialArtistIdSet.toString)

    // 标签ID,标签NAME
    val videoMvTagTable = spark.read.json(videoMvTag)
      .withColumn("mvTagId", $"id")
      .withColumn("mvTagName", $"name")
      .select("mvTagId", "mvTagName")

    val mvSongM = spark.read.option("sep", "\t")
      .csv(mvSongPair)
      .toDF("mvId", "songId")
      .groupBy("mvId")
      .agg(
        collect_set("songId").as("songIdS")
      )
      .withColumn("songIds", setConcat("_tab_")($"songIdS"))
      .select("mvId", "songIds")
      .rdd
      .map(line => (line.getString(0), line.getString(1)))
      .collect()
      .toMap[String, String]
    val mvSongMBroad = spark.sparkContext.broadcast(mvSongM)

    // MvMeta
    val Music_MVMeta_allfieldTable = spark.read.option("sep", "\01")
      .csv(Music_MVMeta_allfield)
      .select(
        $"_c0".cast(LongType).as("mvId"),
        $"_c29".cast(LongType).as("pubTime")
      )

    // MvId， 标签列表
    val videoMvInfoTable = spark.read.json(videoMvInfo)
      .withColumn("artistIds", getIdsFromJsonString($"artists"))
      .withColumn("mvTagIds", getIdsFromListString($"tagIds"))
      .withColumn("mvId", $"id")
      .withColumn("duration", $"duration"/1000)
      .na.fill("", Seq("Type", "SubType", "Resolution"))
      .withColumn("valid", isValidMV_new($"Type", $"SubType", $"Duration", $"Resolution"))
      .filter($"valid")
      .filter($"artistIds"=!=12008067)
      .join(Music_MVMeta_allfieldTable, Seq("mvId"), "inner")
      .na.fill(0l, Seq("pubTime"))
      .withColumn("isNewMv", isNewMv($"pubTime"))
      .select(
        "mvId", "artistIds", "mvTagIds", "area", "duration",
        "caption", "resolution", "style", "type", "name",
        "isNewMv", "pubTime"
      )

    // 分类ID，MV标签ID
    val videoGroupMvTagRelationTable = spark.read.json(videoGroupMvTagRelation)
      .withColumn("mvTagId", $"tagId")
      .select("groupId", "mvTagId")

    val mvTagGroup = videoMvTagTable
      .join(videoGroupMvTagRelationTable, Seq("mvTagId"), "left_outer")
      .na.fill(0, Seq("groupId"))
      .groupBy($"mvTagId", $"mvTagName")
      .agg(
        collect_set($"groupId").as("groupIdS")
      )

    val mvTagGroupM = mvTagGroup
      .collect()
      .map{line =>
        val mvTagId = line.getLong(0)
        val mvTagName = line.getString(1)
        val groupIdS = line.getAs[Seq[Long]](2)
        (mvTagId, (mvTagName, groupIdS))
      }.toMap
    val broadcastMvTagGroupM = spark.sparkContext.broadcast(mvTagGroupM)

    // mv hot score
    val vieoScoreTable = spark.read.parquet(vieoScore)
      .filter($"vType"==="mv" && $"hotScore30days" > 0)
      .withColumn("hotScoreMon", $"hotScore30days")
      .withColumn("mvId", $"videoId")
      .select($"mvId", $"hotScoreMon")

    // mv clickrate
    val videoClickrateTable = spark.read.parquet(videoClickrate)
      .filter($"vType"==="mv")
      .withColumn("originalCtr1Mon", getCtrFromStr($"correctClickrate30days"))
      .withColumn("impressCnt1Mon", getImpressCntFromStr($"correctClickrate30days"))
      .withColumn("playCnt1Mon", getPlayCntFromStr($"correctClickrate30days"))
      .filter($"originalCtr1Mon" > 0)
    val ctr_detail = videoClickrateTable
      .filter($"clickrateType"==="detail")
      .withColumn("smoothDetailCtr1Mon", getSmoothCtrFromStr(1000, 10)($"correctClickrate30days"))
      .withColumn("mvId", $"videoId")
      .select("mvId", "smoothDetailCtr1Mon")
    val ctr_flow = videoClickrateTable
      .filter($"clickrateType"==="flow")
      .withColumn("smoothFlowCtr1Mon", getSmoothCtrFromStr(1000, 10)($"correctClickrate30days"))
      .withColumn("mvId", $"videoId")
      .select("mvId", "smoothFlowCtr1Mon")
    val videoCtrFinal = ctr_detail
      .join(ctr_flow, Seq("mvId"), "outer")

    val finalData_stage1 = videoMvInfoTable
      .withColumn("tagName_groupS", getNameAndGroups(broadcastMvTagGroupM)($"mvTagIds"))
      .withColumn("mvTagName", getInfoFromNameGroups(0)($"tagName_groupS"))
      .withColumn("groupIds", getInfoFromNameGroups(1)($"tagName_groupS"))
      .drop("tagName_groupS")
      .filter($"groupIds"=!="null")
      .join(vieoScoreTable, Seq("mvId"), "left_outer")
      .na.fill(0.0, Seq("hotScoreMon"))
      .withColumn("cutTopHotScore1Mon", cutTop(600)($"hotScoreMon"))
      .join(videoCtrFinal, Seq("mvId"), "left_outer")
      .na.fill(0.01, Seq("smoothDetailCtr1Mon", "smoothFlowCtr1Mon"))
//    finalData_stage1.write.mode(SaveMode.Overwrite).parquet("test")

    val assembler = new VectorAssembler()
      .setInputCols(Array("cutTopHotScore1Mon", "smoothDetailCtr1Mon", "smoothFlowCtr1Mon"))
      .setOutputCol("assembledFeatures")
    val scaler = new MinMaxScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")
    val stage2 = assembler.transform(finalData_stage1)
    val scalerModel = scaler.fit(stage2)
    val stage3 = scalerModel.transform(stage2)

    val finalData4mv = stage3
      //.withColumnRenamed("mvId", "id")
      .withColumn("vType", lit("mv"))
      .withColumn("rawPrediction", dotVectors(Array(0.1, 0.1, 1.0))($"features"))
      .withColumn("songIds", getSongIdsByMap(mvSongMBroad)($"mvId"))

    finalData4mv
      //.withColumnRenamed("id", "mvId")
      .write.parquet(outputPath)

    // 获取concert、eventactivity信息
    val vtagIdNameM = loadVideoTagIdNameM(cmd.getOptionValue("Music_VideoTag"))
    val resourceInput = cmd.getOptionValue("Music_RcmdResource")
    val sidResourcesM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val aidResourcesM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val kwdResourceM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val resourceKwdM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val ridSidAidAtidM = mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]()
    val aidsidResourcesM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val newconcertS = mutable.Set[String]()
    val ridTitleM = getResourceInfoM(resourceInput, vtagIdNameM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidAtidM, aidsidResourcesM, newconcertS)

    flushStringArrayBufferM(sidResourcesM, outputPath4nkv + "/sidResources")
    flushStringArrayBufferM(aidResourcesM, outputPath4nkv + "/aidResources")
    flushStringArrayBufferArrayBufferArrayBufferM(ridSidAidAtidM, outputPath4nkv + "/ridSidAidAtid")
    flushStringArrayBufferM(aidsidResourcesM, outputPath4nkv + "/aidsidResources")
    flushStringStringM(ridTitleM, outputPath4nkv + "/ridTitle")

//    val finalData4resource = ridTitleM
//    spark.createDataFrame(ridTitleM.keySet.toSeq)
    val ridSidAidAtidM_broad = spark.sparkContext.broadcast(ridSidAidAtidM)
    val otherPart = spark.sparkContext.parallelize(ridTitleM.keySet.toSeq)
      .map{idtype =>
        val vType = idtype.split("-")(1)
        var videoBgmIds = "0"
        var artistIds = "0"
        var auditTagIds = "0"
        if (ridSidAidAtidM_broad.value.contains(idtype)) {
          val sidAidAtid = ridSidAidAtidM_broad.value(idtype)
          if (!sidAidAtid._1.isEmpty)
            videoBgmIds = sidAidAtid._1.mkString("_tab_")
          if (!sidAidAtid._2.isEmpty)
            artistIds = sidAidAtid._2.mkString("_tab_")
          if (!sidAidAtid._3.isEmpty)
            auditTagIds = sidAidAtid._3.mkString("_tab_")
        }
        (idtype, vType, videoBgmIds, artistIds, auditTagIds)
      }.toDF("idtype", "vType", "videoBgmIds", "artistIds", "auditTagIds")


    val mvPart = finalData4mv
      .withColumn("idtype", concat_ws("-", $"mvId", $"vType"))
      .withColumnRenamed("songIds", "videoBgmIds")
      .withColumn("auditTagIds", lit("0"))
      .select("idtype", "vType", "videoBgmIds", "artistIds", "auditTagIds")

    val finalDataset = mvPart
      .union(otherPart)
      .withColumn("creatorId", lit(2915298))
      .withColumn("bgmIds", lit("0"))
      .withColumn("userTagIds", lit("0"))
      .withColumn("category", lit("7001_1"))
      .withColumn("isControversial", lit(0))
      .withColumn("rawPrediction", lit(0.0d))
      .withColumn("rawPrediction4newUser", lit(0.0d))
      .withColumn("isMusicVideo", lit(0))
      .withColumn("downgradeScore", lit(0.0d))
      .withColumn("poolStatsScore", lit(0.0d))
      .select(
        "idtype", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds", "auditTagIds", "category", "isControversial"
        ,"rawPrediction", "rawPrediction4newUser", "isMusicVideo", "downgradeScore", "poolStatsScore"
      )
    finalDataset.write.option("sep", "\t").csv(outputPath4nkv + "/csv")
    finalDataset.write.parquet(outputPath4nkv + "/parquet")



  }

  def getResourceInfoM(inputDir:String,
                       vtagIdNameM: mutable.HashMap[String, String],
                       sidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       ridSidAidAtidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                       aidsidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                       newconcertS: mutable.Set[String]
                      ) = {
    val ridTitleM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path)) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getResourceInfoFromFile(bufferedReader, vtagIdNameM, ridTitleM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidAtidM, aidsidResourcesM, newconcertS)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getResourceInfoFromFile(bufferedReader, vtagIdNameM, ridTitleM, sidResourcesM, aidResourcesM, kwdResourceM, resourceKwdM, ridSidAidAtidM, aidsidResourcesM, newconcertS)
      bufferedReader.close()
    }
    ridTitleM
  }

  def getResourceInfoFromFile(reader: BufferedReader,
                              vtagIdNameM: mutable.HashMap[String, String], ridTitleM: mutable.HashMap[String, String],
                              sidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              kwdResourceM: mutable.HashMap[String, mutable.ArrayBuffer[String]], resourceKwdM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                              ridSidAidAtidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                              aidsidResourcesM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
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
          var title = (json \ "title").extractOrElse[String]("")
          title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
          if (resourceType.equals("concert") && artistIds.size == 1) {
            val createTime = (json \ "createTime").extractOrElse[Long](0L)
            if ((NOW_TIME - createTime) < 30 * 24 * 3600 * 1000L)
              newconcertS += resourceId
          }

          if (!resourceType.isEmpty) {
            val ridType = resourceId + "-" + resourceType
            if (!title.isEmpty)
              ridTitleM.put(ridType, title)

            if (songIds != null) {
              for (sid <- songIds) {
                if (!sid.isEmpty) {
                  val resources = sidResourcesM.getOrElse(sid, mutable.ArrayBuffer[String]())
                  sidResourcesM.put(sid, resources)
                  resources.append(ridType)

                  val sidAidAtid = ridSidAidAtidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidAtidM.put(ridType, sidAidAtid)
                  sidAidAtid._1.append(sid)
                }
              }
            }

            if (artistIds != null) {
              for (aid <- artistIds) {
                if (!aid.isEmpty) {
                  val resources = aidResourcesM.getOrElse(aid, mutable.ArrayBuffer[String]())
                  aidResourcesM.put(aid, resources)
                  resources.append(ridType)

                  val sidAidAtid = ridSidAidAtidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidAtidM.put(ridType, sidAidAtid)
                  sidAidAtid._2.append(aid)
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

                  val sidAidAtid = ridSidAidAtidM.getOrElse(ridType, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
                  ridSidAidAtidM.put(ridType, sidAidAtid)
                  sidAidAtid._3.append(tid)
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


  def isNewMv = udf((createTime:Long) => {
    (NOW_TIME - createTime) <= (30 * 24 * 3600 * 1000L)
  })

  def getSongIdsByMap(mvSongMBroad:Broadcast[Map[String, String]]) = udf((mvId:Long) => {
    mvSongMBroad.value.getOrElse(mvId.toString, "0")
  })

  def getIdsFromJsonString = udf((jsonStr:String) => {
    if (jsonStr == null || jsonStr.isEmpty)
      "0"
    else {
      val json = JSON.parseFull(jsonStr)
      json.get.asInstanceOf[List[Map[String, Any]]]
        .map(entry => {
          if (entry.contains("id"))
            entry("id").toString.toFloat.toLong.toString
          else
            "0"
        })
        .mkString("_tab_")
    }
  })

  def getIdsFromListString = udf((listStr:String) => {
    if (listStr.equals("[]") || listStr.isEmpty || listStr == null)
      "null"
    else
      listStr.substring(1, listStr.length-1).replaceAll(",", "_tab_")
  })

  def getNameAndGroups(tagNameGroupsM:Broadcast[collection.immutable.Map[Long,(String, Seq[Long])]]) = udf((mvTagIds:String) => {
    val tagNameS = collection.mutable.Set[String]()
    val groupIdS = collection.mutable.Set[Long]()
    if (!mvTagIds.equals("null")) {
      mvTagIds.split("_tab_").foreach { line =>
        val mvTagId = line.toLong
        if (tagNameGroupsM.value.contains(mvTagId)) {
          val value = tagNameGroupsM.value(mvTagId)
          val tagName = value._1
          val groupIds = value._2
          tagNameS.add(tagName)
          groupIdS ++= groupIds
        }
      }
    }
    groupIdS.remove(0)
    var tagNamesStr = "null"
    var groupIdsStr = "null"
    if (!tagNameS.isEmpty)
      tagNamesStr = tagNameS.mkString("_tab_")
    if (!groupIdS.isEmpty)
      groupIdsStr = groupIdS.mkString("_tab_")
    tagNamesStr + "_spl_" + groupIdsStr
  })

  def getInfoFromNameGroups(pos:Int) = udf((tagName_groupS:String) => {
    tagName_groupS.split("_spl_")(pos)
  })
}
