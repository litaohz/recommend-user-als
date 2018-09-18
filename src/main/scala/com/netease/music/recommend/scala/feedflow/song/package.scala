package com.netease.music.recommend.scala.feedflow

import org.apache.spark.sql.functions._


package object song {

  // udf
  def mergeSets = udf((set1:Seq[String], set2:Seq[String]) => {

    Set(set1)
  })

  def splitByDelim(delim: String) = udf((artistIdsForRec: String) => {

    artistIdsForRec.split(delim)
  })

  def splitByDelim(delim: String, thred: Int) = udf((artistIdsForRec: String) => {

    val ret = artistIdsForRec.split(delim)
    if (ret.length <= thred)
      ret
    else
      ret.slice(0, thred)
  })
}
