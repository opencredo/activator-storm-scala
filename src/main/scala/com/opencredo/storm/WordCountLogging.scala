package com.opencredo.storm

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

trait WordCountLogging extends LazyLogging {

  protected def logWordCount(wordCounts: mutable.Map[String, Int]): Unit = {
    logger.info("Counted words: " + wordCounts.map { case (word: String, count: Int) => s"$word ($count)"}.mkString(", "))
  }
}
