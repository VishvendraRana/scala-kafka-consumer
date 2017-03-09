package rana.kafka.executor

import rana.kafka.consumer.{AutoPartitionManagerKafkaConsumer, MultithreadedKafkaConsumer}

/**
  * Created by rana on 9/3/17.
  */
object ConsumerRebalancer_2 extends App{
  val t2 = new Thread(new AutoPartitionManagerKafkaConsumer(
    MultithreadedKafkaConsumer.getConfig("localhost:9092", "anonymous"), "test"))

  t2.setName("Thread - 2")
  t2.start()
  t2.join()
}
