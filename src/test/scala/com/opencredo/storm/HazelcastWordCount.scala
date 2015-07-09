package com.opencredo.storm

import com.hazelcast.config.{Config => HazelcastConfig}
import com.hazelcast.core.{Hazelcast, HazelcastInstance}

trait HazelcastWordCount {

  val MapNameConfigKey = "wordCountMap"
  val MapName = "wordCount"
  val HazecastAddressConfigKey = "hazelcastAddress"
  val HazecastAddress = "127.0.0.1:5701"

  var hazelcastInstance: HazelcastInstance = _

  def startHazelcast() = {
    val hazelcastConfig = new HazelcastConfig
    hazelcastConfig.getNetworkConfig.setPublicAddress(HazecastAddress)
    hazelcastInstance = Hazelcast.newHazelcastInstance(hazelcastConfig)
  }

  def stopHazelcast(): Unit = {
    hazelcastInstance.shutdown()
  }

  def wordCountFor(word: String): Option[Int] = {
    val wordCountMap = hazelcastInstance.getMap[String, Int](MapName)
    Option(wordCountMap.get(word))
  }
}
