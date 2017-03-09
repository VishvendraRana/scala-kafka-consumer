package rana.kafka.consumer

import java.util
import java.util.Collections

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
  * Created by rana on 8/3/17.
  */
class ConsumerRebalancer(val topic : String, val consumer : KafkaConsumer[String, String]) extends ConsumerRebalanceListener {
  var lastOffset = mutable.Map[Int, Long]()

  def updateOffsetMap(partition : Int, offset : Long) = {
    lastOffset = lastOffset + (partition -> offset)
  }

  def commitOffsets = {
    lastOffset.foreach(elem => {
      val topicPartition = new TopicPartition(topic, elem._1)
      consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(elem._2 + 1)))
    })
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    println("ConsumerRebalancer: Partitions Assigned called....doing nothing!!")
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    println("ConsumerRebalancer: Partitions Revoked called....committing the uncommited data!!")
    commitOffsets
    lastOffset.clear
  }
}
