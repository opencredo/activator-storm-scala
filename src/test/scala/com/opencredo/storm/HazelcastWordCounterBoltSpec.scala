package com.opencredo.storm

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import org.mockito.Mockito
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

class HazelcastWordCounterBoltSpec extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter with HazelcastWordCount {

  val WordField = "word"
  val CountField = "count"
  val Word = "hello"
  val Count = 10

  val config = Map(MapNameConfigKey -> MapName, HazecastAddressConfigKey -> HazecastAddress).asJava
  val topologyContext = mock[TopologyContext]
  val outputCollector = mock[OutputCollector]
  val tuple = mock[Tuple]

  var bolt: BaseRichBolt = _

  before {
    startHazelcast()

    Mockito.reset(topologyContext, outputCollector, tuple)

    bolt = new HazelcastWordCounterBolt
    bolt.prepare(config, topologyContext, outputCollector)

    Mockito.when(tuple.getStringByField(WordField)).thenReturn(Word)
    Mockito.when(tuple.getIntegerByField(CountField)).thenReturn(Count)
  }

  after(stopHazelcast())

  "hazelcast word count bolt" when {
    "receiving the tuple containing count for a new word" should {
      "put the count to the map in Hazelcast" in {
        bolt.execute(tuple)
        wordCountFor(Word) shouldBe Some(Count)
      }
      "acknowledge the tuple" in {
        bolt.execute(tuple)
        Mockito.verify(outputCollector).ack(tuple)
      }
    }
    "receiving the tuple containing count for a existing word" should {
      "put the cumulative count to the map in Hazelcast" in {
        bolt.execute(tuple)
        bolt.execute(tuple)
        bolt.execute(tuple)

        wordCountFor(Word) shouldBe Some(Count * 3)
      }
      "acknowledge the tuple" in {
        bolt.execute(tuple)
        bolt.execute(tuple)
        bolt.execute(tuple)

        Mockito.verify(outputCollector, Mockito.times(3)).ack(tuple)
      }
    }
  }
}
