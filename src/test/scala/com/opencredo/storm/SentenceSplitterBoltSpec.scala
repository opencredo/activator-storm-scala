package com.opencredo.storm

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class SentenceSplitterBoltSpec extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter {

  val SentenceField = "sentence"
  val Words = List("Storm", "is", "a", "distributed", "realtime", "computation", "system")

  val config = Map().asJava
  val topologyContext = mock[TopologyContext]
  val outputCollector = mock[OutputCollector]
  val tuple = mock[Tuple]
  val declarer = mock[OutputFieldsDeclarer]

  var bolt: BaseRichBolt = _

  before {
    reset(topologyContext, outputCollector, tuple, declarer)

    bolt = new SentenceSplitterBolt
    bolt.prepare(config, topologyContext, outputCollector)
  }

  "sentence splitter bolt" when {
    "asked to declare output fields" should {
      "declare word and count fields" in {
        bolt.declareOutputFields(declarer)

        val argumentCaptor = ArgumentCaptor.forClass(classOf[Fields])
        verify(declarer).declare(argumentCaptor.capture())
        argumentCaptor.getValue.toList shouldBe List("word", "count").asJava
      }
    }
    "receiving a tuple containing a sentence separated by spaces" should {
      "acknowledge its receipt" in {
        when(tuple.getStringByField(SentenceField)).thenReturn(Words.mkString(" "))

        bolt.execute(tuple)

        verify(outputCollector).ack(tuple)
      }
      "emit a tuple for every lower case word" in {
        when(tuple.getStringByField(SentenceField)).thenReturn(Words.mkString(" "))

        bolt.execute(tuple)

        Words.foreach { word =>
          verify(outputCollector).emit(tuple, List[AnyRef](word.toLowerCase: java.lang.String, 1: java.lang.Integer).asJava)
        }
      }
    }
    "receiving a tuple containing a sentence separated by spaces and ending with full stop" should {
      "emit a tuple for every lower case word" in {
        when(tuple.getStringByField(SentenceField)).thenReturn(Words.mkString(" ") + '.')

        bolt.execute(tuple)

        Words.foreach { word =>
          verify(outputCollector).emit(tuple, List[AnyRef](word.toLowerCase: java.lang.String, 1: java.lang.Integer).asJava)
        }
      }
    }
    "receiving a tuple containing a sentence separated by commas" should {
      "emit a tuple for every lower case word" in {
        when(tuple.getStringByField(SentenceField)).thenReturn(Words.mkString(","))

        bolt.execute(tuple)

        Words.foreach { word =>
          verify(outputCollector).emit(tuple, List[AnyRef](word.toLowerCase: java.lang.String, 1: java.lang.Integer).asJava)
        }
      }
    }
  }
}
