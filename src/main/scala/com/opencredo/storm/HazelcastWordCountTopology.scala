package com.opencredo.storm

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}
import com.hazelcast.config.{Config => HazelcastConfig}
import com.hazelcast.core.{Hazelcast, IMap}

import scala.collection.JavaConverters._

object HazelcastWordCountTopology extends App with WordCountLogging {

  val hazelcastConfig = new HazelcastConfig
  hazelcastConfig.getNetworkConfig.setPublicAddress("127.0.0.1:5701")
  val hazelcastInstance = Hazelcast.newHazelcastInstance(hazelcastConfig)
  val wordCount: IMap[String, Int] = hazelcastInstance.getMap[String, Int]("wordCount")

  val builder = new TopologyBuilder
  builder.setSpout("generator", new RandomSentenceGeneratorSpout, 1)
  builder.setBolt("splitter", new SentenceSplitterBolt, 3).shuffleGrouping("generator")
  builder.setBolt("counter", new HazelcastWordCounterBolt, 3).shuffleGrouping("splitter")

  val topology: StormTopology = builder.createTopology

  val config = new Config
  config.setDebug(true)
  config.setMaxTaskParallelism(5)
  config.put("wordCountMap", "wordCount")
  config.put("hazelcastAddress", "127.0.0.1:5701")

  val stormCluster = new LocalCluster
  stormCluster.submitTopology("word-count", config, topology)

  Utils.sleep(10000)

  stormCluster.shutdown()

  logWordCount(wordCount.asScala)

  hazelcastInstance.shutdown()
}