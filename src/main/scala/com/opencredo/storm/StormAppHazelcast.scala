package com.opencredo.storm

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}
import com.hazelcast.config.{Config => HazelcastConfig}
import com.hazelcast.core.{Hazelcast, IMap}

import scala.collection.JavaConverters._

object StormAppHazelcast extends App with WordCountLogging {

  val hazelcastInstance = Hazelcast.newHazelcastInstance(new HazelcastConfig)
  val wordCount: IMap[String, Int] = hazelcastInstance.getMap[String, Int]("wordCount")

  val builder = new TopologyBuilder

  builder.setSpout("generator", new RandomSentenceGeneratorSpout, 1)
  builder.setBolt("split", new SplitSentenceBolt, 3).shuffleGrouping("generator")
  builder.setBolt("count", new HazelcastWordCountBolt, 3).shuffleGrouping("split")

  val topology: StormTopology = builder.createTopology

  val config = new Config
  config.setDebug(true)
  config.setMaxTaskParallelism(5)
  config.put("wordCountMap", "wordCount")

  val stormCluster = new LocalCluster
  stormCluster.submitTopology("word-count", config, topology)

  Utils.sleep(10000)

  stormCluster.shutdown()

  logWordCount(wordCount.asScala)

  hazelcastInstance.shutdown()
}