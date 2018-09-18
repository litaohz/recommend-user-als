package com.netease.music.recommend.scala.feedflow.utils

import org.apache.spark.sql.functions._


object Functions4sparkShell {

  def getVideoByImpressGreatorThan(impressCntThred:Long) = udf((clickrateInfo:String) => {
    val info = clickrateInfo.split(":")
    val impressCnt = info(1).toLong
    if (impressCnt >= impressCntThred)
      true
    else
      false
  })
}
