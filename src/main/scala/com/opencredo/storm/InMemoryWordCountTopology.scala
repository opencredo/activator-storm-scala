package com.opencredo.storm

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}

object InMemoryWordCountTopology extends App {

  val builder = new TopologyBuilder
  builder.setSpout("generator", new RandomSentenceGeneratorSpout, 1)
  builder.setBolt("splitter", new SentenceSplitterBolt, 3).shuffleGrouping("generator")
  builder.setBolt("counter", new InMemoryWordCounterBolt, 3).fieldsGrouping("splitter", new Fields("word"))

  val topology: StormTopology = builder.createTopology

  val config = new Config
  config.setDebug(true)
  config.setMaxTaskParallelism(5)

  val stormCluster = new LocalCluster
  stormCluster.submitTopology("word-count", config, topology)

  Utils.sleep(10000)

  stormCluster.shutdown()
}