package rana.kafka.consumer

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}

import scala.collection.JavaConversions._

/**
  * Created by rana on 8/3/17.
  */
class AutoPartitionManagerKafkaConsumer (val props : Properties,
                                         val topic : String = "test") extends Runnable {
  override def run(): Unit = {
    val consumer = new KafkaConsumer[String, String](props)
    val rebalancer = new ConsumerRebalancer(topic, consumer)
    consumer.subscribe(Collections.singletonList(topic), rebalancer)

    while (true) {
      val records = consumer.poll(1000)

      /**
        * Debug logs: for verifying the data
        */
      for (record <- records.iterator()) {
        println("Received Message: ( offset: " + record.offset() + ", key: " + record.key()
          + ", value: " + record.value() + ")"
          + ", Partition: " + record.partition()
          + ", Thread Name: " + Thread.currentThread().getName)
      }

      if (records.count() > 0) {
        for (topicPartition <- records.partitions()) {
          val lastOffset = records.toList.get(records.count() - 1).offset()
          consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)),
            new OffsetCommitCb)

          //set the last offset into the rebalancer
          rebalancer.updateOffsetMap(topicPartition.partition(), lastOffset)
        }
      }
    }
  }
}
