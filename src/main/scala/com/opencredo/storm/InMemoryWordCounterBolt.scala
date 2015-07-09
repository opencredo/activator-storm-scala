package com.opencredo.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple

import scala.collection.mutable

class InMemoryWordCounterBolt extends BaseRichBolt with WordCountLogging {

  private var collector: OutputCollector = _

  private val wordCount = mutable.Map[String, Int]()

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) = {
    this.collector = collector
  }

  override def execute(input: Tuple) = {
    val word = input.getStringByField("word")
    val count = input.getIntegerByField("count")

    wordCount.put(word, wordCount.getOrElse(word, 0) + count)
    collector.ack(input)
    logWordCount(wordCount)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    // nothing
  }

  override def cleanup(): Unit = {
    logWordCount(wordCount)
  }

  private[storm] def wordCountFor(word: String): Option[Int] = wordCount.get(word)
}
