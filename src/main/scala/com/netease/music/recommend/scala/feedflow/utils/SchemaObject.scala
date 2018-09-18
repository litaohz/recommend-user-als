package com.netease.music.recommend.scala.feedflow.utils

import org.apache.spark.sql.types._

object SchemaObject {


  val similarOutputSchema = new StructType(
    Array(
      StructField("id0", StringType, nullable = false),
      StructField("id1", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  val verifiedVideoRcmdMetaTableSchema = StructType(
    Array(
      StructField("videoId", LongType, nullable = false),
      StructField("creatorId", LongType, nullable = false),
      StructField("artistIds", StringType, nullable = false),
      StructField("videoBgmIds", StringType, nullable = false),
      StructField("bgmIds", StringType, nullable = false),
      StructField("userTagIds", StringType, nullable = false),
      StructField("auditTagIds", StringType, nullable = false),
      StructField("expireTime", LongType, nullable = false),
      StructField("category", StringType, nullable = false)
    )
  )

  val userRecallVideoSchema = StructType(Array(
    StructField("userId", StringType, nullable = false),
    StructField("videos", StringType, nullable = false)
  ))
}
