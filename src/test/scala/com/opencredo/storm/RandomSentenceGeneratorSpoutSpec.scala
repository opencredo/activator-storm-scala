package com.opencredo.storm

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Fields, Tuple}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class RandomSentenceGeneratorSpoutSpec extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter {

  val config = Map().asJava
  val topologyContext = mock[TopologyContext]
  val outputCollector = mock[SpoutOutputCollector]
  val tuple = mock[Tuple]
  val declarer = mock[OutputFieldsDeclarer]

  var spout: BaseRichSpout = _

  before {
    reset(topologyContext, outputCollector, tuple, declarer)

    spout = new RandomSentenceGeneratorSpout
    spout.open(config, topologyContext, outputCollector)
  }

  "random sentence generator spout" when {
    "asked to declare output fields" should {
      "declare sentence field" in {
        spout.declareOutputFields(declarer)

        val argumentCaptor = ArgumentCaptor.forClass(classOf[Fields])
        verify(declarer).declare(argumentCaptor.capture())
        argumentCaptor.getValue.toList shouldBe List("sentence").asJava
      }
    }
    "asked for the next tuple" should {
      "emit a random sentence from the list of predefined sentences" in {
        import RandomSentenceGeneratorSpout._

        val argumentCaptor = ArgumentCaptor.forClass(classOf[List[AnyRef]])

        for( i <- 1 to sentences.size * 3) {
          reset(outputCollector)

          spout.nextTuple()

          verify(outputCollector).emit(argumentCaptor.capture().asJava)
          sentences.map(sentence => List[AnyRef](sentence).asJava) should contain(argumentCaptor.getValue)
        }
      }
    }
  }
}
