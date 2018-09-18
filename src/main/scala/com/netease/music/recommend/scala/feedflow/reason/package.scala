package com.netease.music.recommend.scala.feedflow

import org.apache.spark.sql.functions.udf

package object reason {



  def concat_video(sep:String) = udf((videoId:Long, score:Double) => {
    videoId + sep + score.formatted("%.4f")
  })
}
