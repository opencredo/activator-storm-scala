package com.opencredo.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.core.{HazelcastInstance, IMap}

class HazelcastWordCountBolt extends BaseRichBolt {

    private var collector: OutputCollector = null

    private var wordCount: IMap[String, Int] = null

    private var hazelcast: HazelcastInstance = null

    override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) = {
        this.collector = collector
        hazelcast = HazelcastClient.newHazelcastClient(new ClientConfig())
        wordCount = hazelcast.getMap[String, Int](stormConf.get("wordCountMap").asInstanceOf[String])
    }

    override def execute(input: Tuple) = {
        val word = input.getStringByField("word")
        val count = input.getIntegerByField("count")

        wordCount.lock(word)

        try {
            val current = wordCount.get(word)

            Option(current) match {
                case Some(n) => wordCount.put(word, n + count)
                case None => wordCount.put(word, count)
            }
        }

        finally {
            wordCount.unlock(word)
        }

        collector.ack(input)

        println(wordCount)
    }

    override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
        // nothing
    }

    override def cleanup() = {
        hazelcast.shutdown()
    }
}
