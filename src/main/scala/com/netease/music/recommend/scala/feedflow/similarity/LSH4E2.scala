package com.netease.music.recommend.scala.feedflow.similarity

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import breeze.linalg.{DenseVector, Vector, norm}
import breeze.stats.distributions.{Gaussian, Uniform}
import org.apache.spark.HashPartitioner

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class LSH4E2(
            distance: Double,
            lshWith: Double,
            bucketWith: Int = 10,
            bucketSize: Int = 200,
            storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
            parts: Int = 1000,
            lshUpBound: Int = 1000,
            lshRandom: Int = 10,
            mergeSize: Int = 8,
            maxSize: Int = 12
            ) extends Serializable {

  assert(lshWith > 0, s"Width = $lshWith must be greater than 0")
  assert(bucketWith > 0, s"k = $bucketWith must be greater than 0")
  assert(bucketSize > 0, s"l = $bucketSize must be greater than 0")
  assert(lshRandom < lshUpBound, s"lshRandom $lshRandom should be less than lshUpBound $lshUpBound")

  /**
    * 计算lsh
    * @param raw 需要计算的数据
    *
    */
  def lsh[K: ClassTag](raw: RDD[(K, Vector[Double])]): RDD[(K, K, Double)] = {

    // 分区加缓存，用户后面的join
    val data = raw.partitionBy(new HashPartitioner(parts)).persist(storageLevel)
    val nf = java.text.NumberFormat.getIntegerInstance

    // 特征长度
    val vectorWith = data.first._2.length
    assert(vectorWith > 0, s"Vector with is $vectorWith")

    // 生成所有的hash参数
    val normal = Gaussian(0, 1) // 生成随机投影向量
    val uniform = Uniform(0, lshWith) // 生成随机偏差
    val hashSeq: IndexedSeq[(Vector[Double], Double)] = for (i <- 0 until bucketWith * bucketSize)
      yield (DenseVector(normal.sample(vectorWith).toArray), uniform.sample)
    val hashFunctionsBC = data.sparkContext.broadcast(hashSeq)

    // 计算lsh值
    val lshRDD = data.map({ case(id, vector) =>
      val hashFunctions = hashFunctionsBC.value
      val hashValues =  hashFunctions.map({
        case(project: Vector[Double], offset: Double) => (((project dot vector) + offset) / lshWith).floor.toLong
      })
      (id, hashValues.toArray.grouped(bucketWith).toArray)
    }).persist(storageLevel)
    println("LSH Tag")
    lshRDD.take(10).map(x => "%s => %s".format(x._1, x._2.map(_.mkString(" ")).mkString(",")))
      .foreach(println)
    val dataSize = lshRDD.count
    val allPairSize = dataSize * (dataSize - 1) / 2d
    println("LSH Size:" + nf.format(dataSize))

    // 计算所有的LSH桶
    val bucketBuffer = ArrayBuffer[RDD[(K, (K, Double))]]()
    (0 until bucketSize).map(i => {
      val begin = System.currentTimeMillis

      // 统计lsh后每个id的聚集的数据量
      val idGroupRDD = lshRDD.map(x => (x._2(i).mkString(","), x._1))
        .groupByKey(parts)
        .map(_._2.toArray)  // 只需要id，并且转成array，方便后面随机选取，否则非常消耗性能。
        .filter(_.length > 1) // 过滤一个id的情况
        .persist(storageLevel)
      val meanGroup = idGroupRDD.map(_.length).mean
      val stdevGroup = idGroupRDD.map(_.length).stdev
      val maxGroup = idGroupRDD.map(_.length).max
      val pairsCount = idGroupRDD.map(_.length).map(x => x * (x - 1)).sum
      print(
        s"""
           |Mean Group Size: $meanGroup
           |Standart Deviation: $stdevGroup
           |Max Group Size: $maxGroup
           |Pairs Count: $pairsCount
         """.stripMargin)

      // 组合相似对
      val similarPairs = idGroupRDD.flatMap(neighbors => {
        if (neighbors.length <= lshUpBound) {
          // 侯选集不多，排列组合任意两个id
          val pairs = neighbors.combinations(2)
            .map(x => (x(0), x(1))).toArray
          pairs ++ pairs.map(_.swap)
        } else {
          // 候选集过多，随机选取，避免排列组合爆炸
          val rand = new Random(System.currentTimeMillis)
          val allRandomPairs = ArrayBuffer[(K, K)]()
          for (key <- neighbors) {
            val randomRhs = (for (i <- 0 until lshRandom)
              yield neighbors(rand.nextInt(neighbors.length)))
              .distinct.map(x => (key, x))
            allRandomPairs.appendAll(randomRhs)
          }
          allRandomPairs.toArray[(K, K)]
        }
      }).partitionBy(new HashPartitioner(parts)).persist(storageLevel)

      // 删除过多数据
      val reduceSimilarPairs = similarPairs.join(data)
        .map({case (srcKey, (dstKey, srcVec)) => dstKey -> (srcKey, srcVec)})
        .partitionBy(new HashPartitioner(parts))
        .join(data)
        .map({case (dstKey, ((srcKey, srcVec), dstVec)) => srcKey -> (dstKey, norm(srcVec - dstVec, 2))})
        .filter({case (_, (_, dist)) => dist < distance})
        .groupByKey(parts)
        .flatMap({case(srcKey, distList) => distList.toArray.sortBy(_._2).slice(0, maxSize).map({case(dstKey, dist) => (srcKey, (dstKey, dist))})})
        .partitionBy(new HashPartitioner(parts)).persist(storageLevel)

      val currentResultSize = reduceSimilarPairs.count
      val end = System.currentTimeMillis
      val timeCost = (end - begin) / 1000d
      println(s"=====Round $i, Result Size = %s,Time Cost: $timeCost s".format(nf.format(currentResultSize)))
      idGroupRDD.unpersist(false)
      similarPairs.unpersist(false)
      bucketBuffer.append(reduceSimilarPairs)

      // 合并中间表格，节省空间
      if ((bucketBuffer.length >= mergeSize || i == bucketSize) && bucketBuffer.length > 1) {
        val mergeBegin = System.currentTimeMillis

        val merged = data.sparkContext.union(bucketBuffer)
          .groupByKey(parts)
          .flatMap({case(srcKey, dstList) => {
            dstList.toArray
              .distinct
              .sortBy(_._2)
              .slice(0, maxSize)
              .map({case(dstKey, dist) => (srcKey, (dstKey, dist))})
          }}).partitionBy(new HashPartitioner(parts)).persist(storageLevel)

        val mergedCount = merged.count
        println(s"Merged Count : %s".format(nf.format(mergedCount)))

        bucketBuffer.foreach(_.unpersist(false))
        bucketBuffer.clear
        bucketBuffer.append(merged)

        val mergeEnd = System.currentTimeMillis
        val timeCost = (mergeEnd - mergeBegin) / 1000d
        println(s"Merge Time Cost: $timeCost s")
      }
    })

    assert(1 == bucketBuffer.size, "Bucket Size is greater than 1")
    val finalResult = bucketBuffer(0)
      .map({case(srcKey, (dstKey, dist)) => (srcKey, dstKey, dist)})
      .persist(storageLevel)
    bucketBuffer.clear
    val distinctLshPairsSize = finalResult.count
    println(s"Distinct Lsh Pair Size %s, rate = %.3f".format(nf.format(distinctLshPairsSize), distinctLshPairsSize / allPairSize))

    finalResult
  }
}
