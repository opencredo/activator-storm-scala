package com.opencredo.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}

import scala.collection.JavaConverters._

class SentenceSplitterBolt extends BaseRichBolt {

  private var collector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) = {
    this.collector = collector
  }

  override def execute(input: Tuple) = {
    val s = input.getStringByField("sentence")

    s.split("\\W+").map(_.toLowerCase).foreach { word =>
      collector.emit(input, List[AnyRef](word: java.lang.String, 1: java.lang.Integer).asJava)
    }

    collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("word", "count"))
  }
}
