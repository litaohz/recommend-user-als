package com.netease.music.recommend.scala.feedflow.comment

import com.netease.music.recommend.scala.feedflow.comment.LDAPredict.getIdxtokenM
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 资源评论按照tfidf保留关键token
  */
object LDA {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val dataset = spark.read.format("libsvm").load(cmd.getOptionValue("input"))

    // Trains a LDA model.
    val lda = new LDA().setK(128).setMaxIter(10)

    val model = lda.fit(dataset)

    options.addOption("token_idx", true, "token_idx")
    val tokenIdxMapping = spark.read.parquet(cmd.getOptionValue("token_idx"))
    val idxTokenM = getIdxtokenM(tokenIdxMapping.collect())
    println("idxTokenM size:" + idxTokenM.size)
    val idxTokenMBroad = sc.broadcast(idxTokenM)

    model.save(cmd.getOptionValue("output") + "/model")

    val transformed = model.transform(dataset)
    transformed.show(false)
    transformed.write.parquet(cmd.getOptionValue("output") + "/predict")

    val ll = model.logLikelihood(dataset)
    //val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    //println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)


  }

}
