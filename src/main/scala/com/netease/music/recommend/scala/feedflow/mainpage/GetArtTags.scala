package com.netease.music.recommend.scala.feedflow.mainpage

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import com.netease.music.recommend.scala.feedflow.mainpage.GetMainpageRcmdFromResort.getSongArtInfoM
import com.netease.music.recommend.scala.feedflow.mainpage.GetMainpageRcmdFromResort.{comb, getinfoRromMV, parseArts, parseTags}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hzlvqiang on 2017/12/28.
  */
object GetArtTags {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    //val sc = new SparkContext(conf);
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options

    options.addOption("Music_MVMeta_allfield", true, "mv_input")
    options.addOption("Music_Song_artists", true, "music_song_input")
    options.addOption("songtag", true, "songtag")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)
    import spark.implicits._

    val mvInput = cmd.getOptionValue("Music_MVMeta_allfield")
    println("mvInput:" + mvInput)
    val aidTagCntMFromMV = getinfoRromMV(mvInput)

    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    println("songArtInput:" + songArtInput)
    val sidAidsM = mutable.HashMap[String, String]()
    val aidNameM = mutable.HashMap[String, String]()
    getSongArtInfoM(songArtInput, sidAidsM, aidNameM)

    println("load songtag...")
    val songTagInput = cmd.getOptionValue("songtag")
    val aidTagCntM = getAidTagsCntM(songTagInput, sidAidsM)

    flushAidTags(cmd.getOptionValue("output"), aidTagCntMFromMV, aidTagCntM, aidNameM)

  }

  def getinfoRromMV(inputDir: String) = {

    val aidTagsM = new mutable.HashMap[String, mutable.HashMap[String, Int]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getmvArtsMFromFile(bufferedReader, aidTagsM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getmvArtsMFromFile(bufferedReader, aidTagsM)
      bufferedReader.close()
    }
    aidTagsM
  }

  def getmvArtsMFromFile(reader: BufferedReader, aidTagsM: mutable.HashMap[String, mutable.HashMap[String, Int]]
                         ) = {
    var line: String = reader.readLine()
    while (line != null) {
      // ID,Name,Artists,Tags,MVDesc,Valid,AuthId,Status,ArType,MVStype,Subtitle,Caption,Area,Type,SubType,Neteaseonly,Upban,Plays,Weekplays,Dayplays,Monthplays,Mottos,Oneword,Appword,Stars,Duration,Resolution,FileSize,Score,PubTime,PublishTime,Online,ModifyTime,ModifyUser,TopWeeks,SrcFrom,SrcUplod,AppTitle,Subscribe,transName,aliaName,alias,fee
      // println("LINE:" + line)
      if (line != null) {
        val ts = line.split("\01")
        if (ts != null && ts.length >= 3) {
          val mvid = ts(0)
          //println("mvid:" + mvid)
          val artids = parseArts(ts(2))
          //println("artids:" + ts(2))
          val arttags = mutable.ArrayBuffer[String]()
          if (ts.length >= 13) {
            val area = parseTags(ts(12))
            val mvtype = parseTags(ts(9))
            val artype = parseTags(ts(8))
            arttags.appendAll(comb(area, mvtype, artype))
          }
          val anameBuf = new StringBuilder()
          for (aid <- artids) {
            if (!arttags.isEmpty) {
              for (tag <- arttags) {
                val tagCntM = aidTagsM.getOrElse(aid, mutable.HashMap[String, Int]())
                aidTagsM.put(aid, tagCntM)
                tagCntM.put(tag, tagCntM.getOrElse(tag, 0) + 1)
              }
            }
          }
        }
      }
      line = reader.readLine()
    }
  }

  def getAidTagsCntM(inputDir: String, sidAidsM: mutable.HashMap[String, String]) = {
    val aidTagCntsM = mutable.HashMap[String, mutable.HashMap[String, Int]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          appendSongTagFromFile(bufferedReader, sidAidsM, aidTagCntsM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      appendSongTagFromFile(bufferedReader, sidAidsM, aidTagCntsM)
      bufferedReader.close()
    }
    aidTagCntsM
  }

  def appendSongTagFromFile(reader: BufferedReader, sidAidsM: mutable.HashMap[String, String], aidTagCntM:  mutable.HashMap[String, mutable.HashMap[String, Int]]) = {
    var line: String = reader.readLine()
    while (line != null) {
      //101243	民谣_7	1118	3
      val ts = line.split("\t")
      val songid = ts(0)
      val styleLang = ts(1).split("_")
      var langStyle = styleLang(0)
      if (styleLang(1).equals("7")) {
        langStyle = "中文" + langStyle
      } else if (styleLang(1).equals("8")) {
        langStyle = "日语" + langStyle
      } else if (styleLang(1).equals("16")) {
        langStyle = "韩语" + langStyle
      } else if (styleLang(1).equals("96")) {
        langStyle = "欧美" + langStyle
      }
      val aidstr = sidAidsM.getOrElse(songid, "")
      if (!aidstr.isEmpty) {
        for (aid <- aidstr.split(",")) {
          if (!aid.isEmpty) {
            val tagCntM = aidTagCntM.getOrElse(aid, mutable.HashMap[String, Int]())
            aidTagCntM.put(aid, tagCntM)
            tagCntM.put(langStyle, tagCntM.getOrElse(langStyle, 0) + 1)
          }
        }
      }
      line = reader.readLine()
    }
  }

  def getSongArtInfoM(inputDir: String, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String])= {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getSongArtInfoMFromFile(bufferedReader, sidAidM, aidNameM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSongArtInfoMFromFile(bufferedReader, sidAidM, aidNameM)
      bufferedReader.close()
    }
  }

  def getSongArtInfoMFromFile(reader: BufferedReader, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String]) = {
    var line: String = reader.readLine()
    while (line != null) {
      // 59879	1873:阿宝	10846:张冬玲
      if (line != null) {
        val ts = line.split("\t", 2)
        val sid = ts(0)
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
                  if (!aidNameM.contains(aid) && !aname.isEmpty) {
                    aidNameM.put(aid, aname)
                  }
                }
              }
            }
          }
          if (aids.length > 0) {
            sidAidM.put(sid, aids.mkString(","))
          }
        }
      }
      line = reader.readLine()
    }
  }

  def sortStr(tagCntM: mutable.HashMap[String, Int]) = {
    val array = tagCntM.toArray[(String, Int)]
    val res = mutable.ArrayBuffer[String]()
    var top = ""
    for ((tag, cnt) <- array.sortWith(_._2 > _._2) ){
      res.append(tag + ":" + cnt)
      if (top.isEmpty) {
        if (!tag.endsWith("原声")) {
          if (tag.endsWith("男歌手") || tag.endsWith("女歌手")) {
            top = tag.substring(0, tag.length-3)
          } else {
            top = tag
          }
        }
      }
    }
    (res.mkString(","), top)
  }

  def flushAidTags(output: String, aidTagCntMFromMV: mutable.HashMap[String, mutable.HashMap[String, Int]], aidTagCntM: mutable.HashMap[String, mutable.HashMap[String, Int]],
                   aidNameM: mutable.HashMap[String, String]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var bwriter1 = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(output + "/raw"))))
    var bwriter2 = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(output + "/top"))))

    for ((aid, name) <- aidNameM) {
      val tagCntM1 = aidTagCntMFromMV.getOrElse(aid, null)
      var tagstr1 = ""
      var tagstr2 = ""
      val topTags = mutable.ArrayBuffer[String]()
      if (tagCntM1 != null) {
        val (str, top) = sortStr(tagCntM1)
        tagstr1 = str
        if (top != null && !top.isEmpty) {
          topTags.append(top)
        }
      }
      val tagCntM2 = aidTagCntM.getOrElse(aid, null)
      if (tagCntM2 != null) {
        val (str, top) = sortStr(tagCntM2)
        tagstr2 = str
        if (top != null && !top.isEmpty) {
          topTags.append(top)
        }
      }
      bwriter1.write(aid + "\t" + name + "\t" + tagstr1 + "\t" + tagstr2 + "\n")
      if (!topTags.isEmpty) {
        bwriter2.write(aid + "\t" + name + "\t" + topTags.mkString(",") + "\n")
      }
    }
    bwriter1.close()
    bwriter2.close()
  }

}
