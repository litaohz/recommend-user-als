package com.netease.music.recommend.scala.feedflow

import com.netease.music.recommend.scala.feedflow.utils.ABTest
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

package object userbased {

  val testUserSet = mutable.HashSet[Long](359792224, 303658303, 135571358, 2915298, 85493186, 41660744, 17797935)

  def abtestGroupName(configText: Array[String], abtestExpName: String, uid:String) = {
    val musicABTest = new ABTest(configText)
    musicABTest.abtestGroup(uid.toLong, abtestExpName)
  }

  def abtestGroupName_udf(configText: Array[String], abtestExpName: String) =udf((uid:String) => {
    val musicABTest = new ABTest(configText)
    musicABTest.abtestGroup(uid.toLong, abtestExpName)
  })
}
