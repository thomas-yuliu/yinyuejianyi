package mysparkproject.recommender2016.streamingRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object StreamingRecommender {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Recommender")
    val ssc = new StreamingContext(conf, Seconds(5))//will be per minute in real case
    
    val zkQuorum = "localhost"
    val consumerGroup = "streamingRecommender"
    val topicMap = collection.immutable.HashMap("test"->1)
    
    val msgs = KafkaUtils.createStream(ssc, zkQuorum, consumerGroup, topicMap).map(_._2)
    msgs.saveAsTextFiles("/Users/yliu/deployment/recommendationProject/streamingResult/sresult", "txt")
    
    /*
    //parse daily events into tuple
    val eventTuples = eventLines.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
        (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
    })
    eventTuples.saveAsTextFiles("/Users/yliu/deployment/recommendationProject/streamingResult/sresult", "txt")
    */
    ssc.start()
    ssc.awaitTerminationOrTimeout(10*1000)//10 sec
  }
}