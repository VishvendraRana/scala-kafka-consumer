package rana.kafka.executor

import rana.kafka.consumer.MultithreadedKafkaConsumer

/**
  * Created by rana on 8/3/17.
  */
object MultiThreadedConsumer_2 extends App {
  val t1 = new Thread(new MultithreadedKafkaConsumer("test", 0, MultithreadedKafkaConsumer.getConfig("localhost:9092", "anonymous")))
  t1.setName("Thread - 3")
  t1.start()
  t1.join()
}
