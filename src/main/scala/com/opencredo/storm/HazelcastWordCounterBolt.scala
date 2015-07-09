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

class HazelcastWordCounterBolt extends BaseRichBolt with WordCountLogging {

  private var collector: OutputCollector = _
  private var wordCount: IMap[String, Int] = _
  private var hazelcastClient: HazelcastInstance = _

  override def prepare(config: util.Map[_, _], context: TopologyContext, collector: OutputCollector) = {
    this.collector = collector
    val clientConfig = new ClientConfig
    clientConfig.getNetworkConfig.addAddress(config.get("hazelcastAddress").asInstanceOf[String])
    hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig)
    wordCount = hazelcastClient.getMap[String, Int](config.get("wordCountMap").asInstanceOf[String])
  }

  override def execute(input: Tuple) = {
    val word = input.getStringByField("word")
    val count = input.getIntegerByField("count")

    wordCount.lock(word)
    try {
      val total = Option(wordCount.get(word))
      wordCount.put(word, total.getOrElse(0) + count)
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
    hazelcastClient.shutdown()
  }
}
