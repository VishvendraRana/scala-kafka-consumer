package rana.kafka.consumer

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}

import scala.collection.JavaConversions._

/**
  * Created by rana on 7/3/17.
  */
class MultithreadedKafkaConsumer(val topic : String = "test",
                                 val partition : Int = 0,
                                 val props : Properties) extends Runnable {
  override def run(): Unit = {
    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(topic))

    while (true) {
      val records = consumer.poll(1000)

      for (record <- records) {
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
        }
      }
    }
  }
}

object MultithreadedKafkaConsumer {
  def getConfig(brokerServer : String, groupId : String) : Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    props
  }
}