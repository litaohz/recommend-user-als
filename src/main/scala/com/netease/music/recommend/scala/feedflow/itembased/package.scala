package com.netease.music.recommend.scala.feedflow

import java.io.{BufferedReader, IOException, InputStreamReader}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

package object itembased {

  val recommendableResourcetypeS = Set("video", "mv")

  def isFilterBySourceThredRule(sourceS: scala.collection.mutable.Set[String], resonRecallcntM: scala.collection.mutable.Map[String, Int], recall_thred4reason: Int): Boolean = {
    var filter = false
    sourceS.foreach{sourceIdtype =>
      if (!filter) {
        val recallcnt_eachReason = resonRecallcntM.getOrElse(sourceIdtype, 0)
        if (recallcnt_eachReason >= recall_thred4reason)
          filter = true
      }
    }
    filter
  }

  def updateResonRecallcntM(sourceS: scala.collection.mutable.Set[String], resonRecallcntM: scala.collection.mutable.Map[String, Int]) = {
    sourceS.foreach{sourceIdtype =>
      val recallcnt_eachReason = resonRecallcntM.getOrElse(sourceIdtype, 0)
      resonRecallcntM.put(sourceIdtype, recallcnt_eachReason+1)
    }
  }

  def loadLines2SetChar(fs: FileSystem, path: Path): collection.mutable.HashSet[Char] = {
    val set = collection.mutable.HashSet[Char]()
    //    val path = new Path(pathStr)
    if (fs.isDirectory(path)) {
      for (st <- fs.listStatus(path)) {
        set ++= loadLines2SetChar(fs, st.getPath)
      }
    } else {
      val hdfsInStream = fs.open(path)
      val br = new BufferedReader(new InputStreamReader(hdfsInStream))
      var line = br.readLine()
      //        try {
      while (line != null) {
        if (!line.isEmpty) {
          set.add(line.charAt(0))
        }
        line = br.readLine()
      }
      br.close()
      //        } catch {
      //          case ex:IOException => println(line + "=>" + ex)
      //        }
    }
    set
  }

  def norm(str: String, stopwords: collection.mutable.HashSet[Char]): String = {
    val sb = new StringBuilder()
    for (c <- str.toCharArray) {
      if (!stopwords.contains(c))
        sb.append(c)
    }
    sb.toString()
  }
}
