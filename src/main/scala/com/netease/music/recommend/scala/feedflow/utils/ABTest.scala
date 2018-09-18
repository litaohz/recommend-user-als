package com.netease.music.recommend.scala.feedflow.utils

import com.netease.music.app.abtest.pub.{ABTestGroupService, ABTestGroupServiceImpl}
import com.netease.music.app.abtest.pub.dto.{PubABTestGroup, PubBackendABTestConfig}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class ABTest(configText:Array[String]) extends Serializable {

//  val stream : InputStream = getClass.getResourceAsStream("/abtest_config")
//  val configText = Source.fromInputStream(stream).getLines()

  val configList = configText.map(line => {

    // println(line)

    val ts = line.split("\t")
    if (ts.length < 9)

      null

    else {

      val config: PubBackendABTestConfig = new PubBackendABTestConfig()

      implicit val formats = DefaultFormats

      val codeName = ts(2)
      val groupListJson = ts(5)
      val status = ts(8).toInt

      val jsonArray = parse(groupListJson)

      val groups:java.util.List[PubABTestGroup] = jsonArray.values.asInstanceOf[List[Map[String, _]]].map(line => {
        val group = new PubABTestGroup()
        val groupName = line.getOrElse("groupName", "unknown").asInstanceOf[String]
        val probability = line.getOrElse("probability", 0.0).asInstanceOf[Double]
        group.setGroupName(groupName)
        group.setProbability(probability)
        group
      })

      // 3处set
      config.setGroupList(groups)
      config.setCodename(codeName)
      config.setStatus(status)

      config

    }

  }).toArray

  val config = configList(0)

  val abTestGroupService: ABTestGroupService = new ABTestGroupServiceImpl

  def getGroupName(uid:Long) = {
    val dto = abTestGroupService.group(uid, config)
    dto.getAbtestgroup
  }
  def abtestGroup(uid:Long, expName:String) = {
    var group = "control"
    for (cf <- configList) {
      if (cf != null && !"null".equals(cf)) {
        try {
          val dto = abTestGroupService.group(uid, cf)
          if (dto.getAbtestname.equals(expName)) {
            group = dto.getAbtestgroup
          }
        } catch {
          case ex: Exception => println(cf + "=>" + ex)
        }
      }
    }
    group
  }
  def getCodeGroupNames (uid:Long) = {
    val codeGroupList = new ArrayBuffer[String]();
    for (cf <- configList) {
      if (cf != null && !"null".equals(cf)) {
        try {
          val dto = abTestGroupService.group(uid, cf)
          codeGroupList.append(dto.getAbtestname + "-" + dto.getAbtestgroup)
        } catch {
          case ex: Exception => println(cf + "=>" + ex)
        }
      }
    }
    codeGroupList
    // dto.getAbtestname + "-" + dto.getAbtestgroup
  }
}
