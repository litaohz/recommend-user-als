package com.netease.music.recommend.scala.feedflow

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import scala.collection.mutable._

package object group {

  case class TagMap(tagId:String, tagName:String)



  def getGroups(broadcaseTagGroupM:Broadcast[HashMap[String, Set[String]]]) = udf((tagNameStr:String) => {

    val videoGroupsS = Set[String]()
    tagNameStr.split("_tab_").foreach { tag =>
      if (broadcaseTagGroupM.value.contains(tag)) {
        val groups = broadcaseTagGroupM.value.get(tag).get
        videoGroupsS ++= groups
      }
    }
    if (videoGroupsS.isEmpty)
      "null"
    else
      videoGroupsS.mkString("_tab_")
  })

  def mergeTagIds = udf((userTagIds:String, auditTagIds:String) => {

    val videoTagsS = Set[String]()
    userTagIds.split("_tab_").foreach{ tag => videoTagsS.add(tag) }
    auditTagIds.split("_tab_").foreach{ tag => videoTagsS.add(tag) }
    //去除0
    videoTagsS.remove("0")

    if (videoTagsS.isEmpty)
      "null"
    else
      videoTagsS.mkString("_tab_")
  })
}
