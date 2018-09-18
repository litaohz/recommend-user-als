package com.netease.music.recommend.scala.feedflow.quota

import com.netease.music.recommend.scala.feedflow.utils.ABTest
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 获取分在alg的group的用户
  * Created by zhangying5 on 2018/7/5.
  */
object GetUserAlgGroup {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("users", true, "input user")
    options.addOption("alg", true, "alg name")
    options.addOption("group", true, "group name")
    options.addOption("abtest", true, "abtest")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val usersInput = cmd.getOptionValue("users")
    val algName = cmd.getOptionValue("alg")
    val groupName = cmd.getOptionValue("group")
    val abtestInput = cmd.getOptionValue("abtest")
    val outputPath = cmd.getOptionValue("output")

    val configText = spark.read.textFile(abtestInput)
      .filter(x => x.split("\t")(2).equals(algName))
      .collect()
    println("configText before ")
    for (cf <- configText) {
      println(cf)
    }

    // TODO 说明中有换行的特殊处理
    for (i <- 0 until configText.length) {
      if (configText(i).contains("videorcmd-specialrcmd")) {
        configText(i) = configText(i) + "\t补充说明\t1"
      }
    }
    println("configText after ")
    for (cf <- configText) {
      println(cf)
    }

    val musicABTest = new ABTest(configText)
    val uid = "268130986" //zhangying 163
    val cg = musicABTest.getCodeGroupNames(uid.toLong)(0)
    val g = cg.split("-")(2)
    println("equal:" + g.equals(groupName))
    println("example:" + g)

    val usersTable = spark.sparkContext.textFile(usersInput)
    println("alg number:" + usersTable.count().toString)

    val usersTable_group = usersTable
      .filter { line =>
        val uid = line.split("\t")(0)
        val musicABTest = new ABTest(configText)
        val codeGroup = musicABTest.getCodeGroupNames(uid.toLong)(0)
        val group = codeGroup.split("-")(2)
        group.equals(groupName)
      }
    println("group number:" + usersTable_group.count().toString)

    usersTable_group.saveAsTextFile(outputPath)

  }
}


