package com.opencredo.storm

import backtype.storm.generated.StormTopology
import backtype.storm.testing.{CompleteTopologyParam, MkClusterParam, MockedSources, TestJob}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Values
import backtype.storm.{Config, ILocalCluster, Testing}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class HazelcastWordCountTopologyIntegrationSpec extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter with HazelcastWordCount {

  before(startHazelcast())

  after(stopHazelcast())

  "work count topology using Hazelcast" when {
    "executed in the local Storm cluster" should {
      "result in word counts being stored in Hazelcast" in {
        val mkClusterParam = new MkClusterParam()
        mkClusterParam.setSupervisors(2)
        mkClusterParam.setPortsPerSupervisor(5)
        mkClusterParam.setDaemonConf(Map(Config.SUPERVISOR_ENABLE -> true, Config.TOPOLOGY_ACKER_EXECUTORS -> 1).asJava)

        Testing.withLocalCluster(mkClusterParam, new TestJob() {
          override def run(cluster: ILocalCluster) {
            val builder = new TopologyBuilder
            builder.setSpout("generator", new RandomSentenceGeneratorSpout, 1)
            builder.setBolt("split", new SentenceSplitterBolt, 3).shuffleGrouping("generator")
            builder.setBolt("count", new HazelcastWordCounterBolt, 3).shuffleGrouping("split")

            val topology: StormTopology = builder.createTopology

            val mockedSources = new MockedSources()
            mockedSources.addMockData("generator", new Values("hello world"), new Values("hello world"), new Values("hello world"))

            val config = new Config
            config.setMaxTaskParallelism(5)
            config.setNumWorkers(2)
            config.put("wordCountMap", "wordCount")
            config.put("hazelcastAddress", "127.0.0.1:5701")

            val completeTopologyParam = new CompleteTopologyParam()
            completeTopologyParam.setMockedSources(mockedSources)
            completeTopologyParam.setStormConf(config)

            Testing.completeTopology(cluster, topology, completeTopologyParam)

            wordCountFor("hello") shouldBe Some(3)
            wordCountFor("world") shouldBe Some(3)
          }
        })
      }
    }
  }
}
