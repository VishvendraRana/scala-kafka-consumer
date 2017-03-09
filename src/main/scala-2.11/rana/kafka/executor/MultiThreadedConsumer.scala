package rana.kafka.executor

import rana.kafka.consumer.MultithreadedKafkaConsumer

/**
  * Created by rana on 8/3/17.
  */
object MultiThreadedConsumer_1 extends App {
  val t1 = new Thread(new MultithreadedKafkaConsumer("test", 0, MultithreadedKafkaConsumer.getConfig("localhost:9092", "anonymous")))
  val t2 = new Thread(new MultithreadedKafkaConsumer("test", 1, MultithreadedKafkaConsumer.getConfig("localhost:9092", "anonymous")))

  t1.setName("Thread - 1")
  t2.setName("Thread - 2")

  t1.start()
  t2.start()

  t1.join()
  t2.join()
}
