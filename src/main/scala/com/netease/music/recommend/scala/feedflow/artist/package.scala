package com.netease.music.recommend.scala.feedflow

package object artist {


  case class OnlineVideoInfo(videoId:Long, vType:String, alg:String, rawPrediction:Double, recallType:Int)
  case class OnlineVideoInfoNew(videoInfo:String, rawPrediction:Double)

}
