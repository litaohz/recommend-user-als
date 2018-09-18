package com.netease.music.recommend.scala.feedflow

import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by hzzhangjunfei1 on 2017/8/1.
  */
object GetVideoEventIndex {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate

    val options = new Options
//    options.addOption("verifiedVideoRcmdMeta", true, "input directory")
    options.addOption("eventResourceInfoPool", true, "input directory")
//    options.addOption("controversialFiguresFromCreator", true, "input directory")
//    options.addOption("controversialFiguresFromContent", true, "input directory")
//    options.addOption("controversialFiguresInput", true, "input directory")

    options.addOption("output", true, "output directory")
    options.addOption("outputNkv", true, "outputNkv directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

//    val verifiedVideoRcmdMetaInput = cmd.getOptionValue("verifiedVideoRcmdMeta")
    val eventResourceInfoPoolInput = cmd.getOptionValue("eventResourceInfoPool")
//    val controversialFiguresFromCreatorInput = cmd.getOptionValue("controversialFiguresFromCreator")
//    val controversialFiguresFromContentInput = cmd.getOptionValue("controversialFiguresFromContent")
//    val controversialFiguresInput = cmd.getOptionValue("controversialFiguresInput")

    val outputPath = cmd.getOptionValue("output")
    val outputNkvPath = cmd.getOptionValue("outputNkv")

    import spark.implicits._

//    val controversialFiguresTable = spark.sparkContext.textFile(controversialFiguresInput)
//      .map(line => {
//        val info = line.split("\t")
//
//        val controversialArtistidForMarking = info(0).toLong
//        ControversialArtistidForMarking(controversialArtistidForMarking)
//      })
//      .toDF
//      .dropDuplicates("controversialArtistidForMarking")
//
//    val controversialFiguresFromContentTable = spark.sparkContext.textFile(controversialFiguresFromContentInput)
//      .map(line => {
//        val info = line.split("\t")
//
//        val eventId = info(0).toLong
//        val artists = info(1)
//        ControversialEventFromKeywordContent(eventId, artists)
//      })
//      .toDF
//
//    val controversialFiguresFromCreatorTable = spark.sparkContext.textFile(controversialFiguresFromCreatorInput)
//      .map(line => {
//        val info = line.split("\t")
//
//        val creatorIdFromNickname = info(0).toLong
//        val artists = info(1)
//        ControversialEventFromKeywordCreator(creatorIdFromNickname, artists)
//      })
//      .toDF

//    val verifiedVideoRcmdMetaTable = spark.read
//      .option("sep", "\t")
//      .schema(verifiedVideoRcmdMetaTableSchema)
//      .csv(verifiedVideoRcmdMetaInput)

    val eventResourceInfoPoolTable = spark.read.parquet(eventResourceInfoPoolInput)
        .filter($"resourceType" === "video" || $"resourceType" === "mv")
        .filter($"isVerifiedEvent" === 1)
//        .withColumn("videoId", $"resourceId")
        .withColumn("videoId", $"eventId")
        .withColumn("vType", $"resourceType")
        .withColumn("artistIds", $"artistIdsForRec")
        .withColumn("videoBgmIds", $"songIdsForRec")
        .withColumn("category", $"eventCategory")
        .withColumn("userTagIds", getDefaultColumn("null")())
        .withColumn("auditTagIds", getDefaultColumn("null")())
        .withColumn("bgmIds", getDefaultColumn("null")())
        .withColumn("title", getDefaultColumn("null")())
        .withColumn("description", getDefaultColumn("null")())
        .withColumn("isControversial", isControversialEvent($"isControversialEvent", $"isControversialFigure"))
        .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
               ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
               ,"score", "title", "description")



    val videoInfoPoolTable =
      eventResourceInfoPoolTable
//    val videoInfoPoolTable = verifiedVideoRcmdMetaTable
//      .join(controversialFiguresFromContentTable, Seq("eventId"), "left_outer")
//      .join(controversialFiguresFromCreatorTable, $"creatorId" === $"creatorIdFromNickname", "left_outer")
//      .na.fill("0", Seq("artistsFromKeywordContent","artistsFromKeywordContentForRec", "artistsFromKeywordCreator"))
//      .join(controversialFiguresTable, $"artistId" === $"controversialArtistidForMarking", "left_outer")
//      .na.fill(0, Seq("controversialArtistidForMarking"))
//      .withColumn("isControversial", isControversialVideo($"controversialArtistidForMarking"))
//      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
//             ,"bgmIds", "userTagIds" ,"auditTagIds", "category"
//             ,"isControversial")

    videoInfoPoolTable.write.parquet(outputPath + "/parquet")
    videoInfoPoolTable.write.option("sep", "\t").csv(outputPath + "/csv")

    videoInfoPoolTable
      .write.option("sep", "\t").csv(outputNkvPath)

  }
}
