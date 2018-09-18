package com.netease.music.recommend.scala.feedflow.tag
import org.apache.commons.cli.{ Options, PosixParser }
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{ MinMaxScaler, VectorAssembler }
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{ Word2Vec, Word2VecModel }
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import java.io.ObjectOutputStream;
import org.apache.hadoop.conf.Configuration;

/**
  * word2vec
  */
object Word2Vect {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val options = new Options()
    options.addOption("input", true, "input")
    options.addOption("output", true, "output")
    options.addOption("iteration", true, "iteration")
    options.addOption("window", true, "window")
    options.addOption("vectsize", true, "vectsize")
    options.addOption("mincount", true, "mincount")
    options.addOption("partition", true, "partition")
    // options.addOption("modelOutput", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputPath = cmd.getOptionValue("input")
    val outputPath = cmd.getOptionValue("output")
    val iteration = Integer.parseInt(cmd.getOptionValue("iteration"))
    val window = Integer.parseInt(cmd.getOptionValue("window"))
    val vectsize = Integer.parseInt(cmd.getOptionValue("vectsize"))
    val mincount = Integer.parseInt(cmd.getOptionValue("mincount"))
    val partition = Integer.parseInt(cmd.getOptionValue("partition"))

    println("#PARAMS#:iteration:$iteration, window:$window, vectsize:$vectsize")

    // val modelOutput = cmd.getOptionValue("modelOutput")
    println("Load: $inputPath")
    val inputData = sc.textFile(inputPath).map(line => line.split(",").toSeq).cache()

    println("Init w2v...")
    val word2vec = new Word2Vec().setMinCount(mincount).setNumPartitions(partition).setNumIterations(iteration).setWindowSize(window).setVectorSize(vectsize)
    // val word2vec = new Word2Vec().setNumIterations(20).setWindowSize(5).setVectorSize(100)
    println("Fit w2v...")
    val model = word2vec.fit(inputData)

    val keys = sc.parallelize(model.getVectors.keys.toList)
    val result = keys.map(word =>  (word, model.findSynonyms(word, 60)) )

    result.map(wordSim => wordSim._1 + "\t" + wordSim._2.mkString(",") ).saveAsTextFile(outputPath + "/sim")

    try {
      val outputObject = new Path( outputPath + "/object")
      val hdfs : FileSystem = FileSystem.get(new Configuration)
      val outputstream : FSDataOutputStream = hdfs.create(outputObject)
      val oos = new ObjectOutputStream(outputstream)
      oos.writeObject(model);
      oos.close();
    } catch  {
      case e : Exception => println("Self Expception:" + e.toString())
    }

    // Save and load model
    println(s"Output: $outputPath");

    // model.save(sc, outputPath + "/model")
  }
}