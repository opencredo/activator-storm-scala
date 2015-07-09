package com.opencredo.storm

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils

import scala.collection.JavaConverters._
import scala.util.Random

class RandomSentenceGeneratorSpout extends BaseRichSpout {

  import RandomSentenceGeneratorSpout._

  private var collector: SpoutOutputCollector = _

  override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this.collector = collector
  }

  override def nextTuple(): Unit = {
    collector.emit(List[AnyRef](nextSentence).asJava)
    Utils.sleep(100)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("sentence"))
  }
}

object RandomSentenceGeneratorSpout {

  private val random = new Random

  private[storm] val sentences = List(
    "Storm is a distributed realtime computation system.",
    "The logic for a realtime application is packaged into a Storm topology.",
    "The stream is the core abstraction in Storm.",
    "A spout is a source of streams in a topology.",
    "All processing in topologies is done in bolts.",
    "Bolts can do anything from filtering, functions, aggregations, joins, talking to databases, and more.",
    "Part of defining a topology is specifying for each bolt which streams it should receive as input.")

  private def nextSentence: String = sentences(random.nextInt(sentences.size - 1))
}