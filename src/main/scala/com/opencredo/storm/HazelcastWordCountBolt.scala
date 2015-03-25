package com.opencredo.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.core.{HazelcastInstance, IMap}
import scala.collection.JavaConverters._

class HazelcastWordCountBolt extends BaseRichBolt with WordCountLogging {

  private var collector: OutputCollector = _

  private var wordCount: IMap[String, Int] = _

  private var hazelcast: HazelcastInstance = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) = {
    this.collector = collector
    hazelcast = HazelcastClient.newHazelcastClient(new ClientConfig)
    wordCount = hazelcast.getMap[String, Int](stormConf.get("wordCountMap").asInstanceOf[String])
  }

  override def execute(input: Tuple) = {
    val word = input.getStringByField("word")
    val count = input.getIntegerByField("count")

    wordCount.lock(word)

    try {
      val current = Option(wordCount.get(word))

      current match {
        case Some(n) => wordCount.put(word, n + count)
        case None => wordCount.put(word, count)
      }

    } finally {
      wordCount.unlock(word)
    }

    collector.ack(input)

    logWordCount(wordCount.asScala)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    // nothing
  }

  override def cleanup() = {
    hazelcast.shutdown()
  }
}
