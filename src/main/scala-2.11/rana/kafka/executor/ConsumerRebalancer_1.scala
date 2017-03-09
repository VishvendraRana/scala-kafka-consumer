package rana.kafka.executor

import rana.kafka.consumer.{AutoPartitionManagerKafkaConsumer, MultithreadedKafkaConsumer}

/**
  * Created by rana on 9/3/17.
  */
object ConsumerRebalancer_1 extends App {
  val t1 = new Thread(new AutoPartitionManagerKafkaConsumer(
    MultithreadedKafkaConsumer.getConfig("localhost:9092", "anonymous"), "test"))

  t1.setName("Thread - 1")
  t1.start()
  t1.join()
}
