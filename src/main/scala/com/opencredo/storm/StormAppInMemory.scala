package com.opencredo.storm

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}

object StormAppInMemory extends App {

  val builder = new TopologyBuilder

  builder.setSpout("generator", new RandomSentenceGeneratorSpout, 1)
  builder.setBolt("split", new SplitSentenceBolt, 3).shuffleGrouping("generator")
  builder.setBolt("count", new InMemoryWordCountBolt, 3).fieldsGrouping("split", new Fields("word"))

  val topology: StormTopology = builder.createTopology

  val config = new Config
  config.setDebug(true)
  config.setMaxTaskParallelism(5)

  val stormCluster = new LocalCluster
  stormCluster.submitTopology("word-count", config, topology)

  Utils.sleep(10000)

  stormCluster.shutdown()
}