package com.opencredo.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple

import scala.collection.mutable

class InMemoryWordCountBolt extends BaseRichBolt {

    private var collector: OutputCollector = null

    private val wordCount = mutable.Map[String, Int]()

    override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) = {
        this.collector = collector
    }

    override def execute(input: Tuple) = {
        val word = input.getStringByField("word")
        val count = input.getIntegerByField("count")

        wordCount.get(word) match {
            case Some(n) => wordCount(word) += count
            case None => wordCount(word) = count
        }

        collector.ack(input)
        
        println(wordCount)
    }

    override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
        // nothing
    }
}
