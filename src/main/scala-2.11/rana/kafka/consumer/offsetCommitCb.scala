package rana.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer.{CommitFailedException, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
  * Created by rana on 8/3/17.
  */
class OffsetCommitCb extends OffsetCommitCallback {
  override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
    if (exception == null) {
      for (offset <- offsets) {
        println("Topic: " + offset._1.topic() + ", Partition: " + offset._1.partition()
          + ", offset: " + offset._2.offset() + " is committed!!")
      }
    } else {
      exception match {
        case ex : CommitFailedException => throw new Exception("CommitFailedException: Commit is failed and can't be retried!!"
          + "\nSome active group with same groupId is using auto management!!" + ex.fillInStackTrace())
        case _ => throw new Exception("Some Kafka exception occurred while committing the offsets!!" + exception.printStackTrace())
      }
    }
  }
}
