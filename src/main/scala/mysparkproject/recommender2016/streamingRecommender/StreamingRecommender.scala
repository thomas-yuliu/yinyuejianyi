package mysparkproject.recommender2016.streamingRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import mysparkproject.recommender2016.util.ConfigLoader

object StreamingRecommender {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Recommender")
    val ssc = new StreamingContext(conf, Seconds(5)) //will be per minute in real case

    val zkQuorum = "localhost"
    val consumerGroup = "streamingRecommender"
    val topicName = ConfigLoader.query("daily_rating_kafka_topic")
    val topicMap = collection.immutable.HashMap(topicName -> 1)

    val msgs = KafkaUtils.createStream(ssc, zkQuorum, consumerGroup, topicMap).map(_._2)
    //msgs.saveAsTextFiles("/Users/yliu/deployment/recommendationProject/streamingResult/sresult", "txt")

    //parse daily events into tuple
    val eventPairs = msgs.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
        trackid -> (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
    })

    val groupByTrack = eventPairs.map(event => event._1 -> List(event._2)) //convert to list to make reduce function easier
      .reduceByKey((event1, event2) => event1 ++ event2)
    //now in the form of trackId -> list(event)
    val computedWeight = groupByTrack.map {
      case (trackId, events) => {
        //mock score computation
        val randomGenerator = scala.util.Random
        val eventScores = events.map(event => (event._1, randomGenerator.nextInt(10)))
        trackId -> eventScores
      }
    }

    //mock fetching rec vector
    val fetchedTrackVector = computedWeight.map { trackJson => (trackJson._1, (0.1, 0.1, 0.1), trackJson._2)}
    
    //mock updating track vector with ML ALS, using data needed for training ALS in DB
    val updatedVector = fetchedTrackVector.map(trackJson => (trackJson._1, (0.1, 0.1, 0.1), trackJson._3))

    //mock updating track vector with ML ALS, using data needed for training ALS in DB
    val findRecVector = updatedVector.map(trackJson => (trackJson._1, List((0.1, 0.1, 0.1),(0.2, 0.2, 0.2),(0.3, 0.3, 0.3)), trackJson._3))

    //mock calculating cosine similarity for each rec vector
    val cosSimForEachRec = findRecVector.map(trackJson => (trackJson._1, List((0.1, 0.1, 0.1, 299),(0.2, 0.2, 0.2, 300),(0.3, 0.3, 0.3, 150)), trackJson._3))

    //convert (track, rec tracks with score, events) to (event, weight, rec tracks with score)
    val eventRecs = cosSimForEachRec.flatMap{case(trackId, recs, events) =>
      events.map(event => (event._1, event._2, recs))
    }
    
    //mock calculating rec score for each rec
    val eventRecScores = eventRecs.map(event => {
      val recWithScore = event._3.map(rec => (rec._1, rec._4 * event._2))
      (event._1, recWithScore)
    })
    
    //mock filtering with bloom filter
    val afterFiltering = eventRecScores.filter(event => event._1==event._1)
    
    //mock grouping by user
    val userRecs = afterFiltering.reduceByKey((recList1, recList2) => recList1 ++ recList2)
    
    //mock reading recs from batch for each user. will add some cassandra read here
    val userRecsWithBatchRecs = userRecs.map(userRec => userRec)
    
    //mock recompute rec track score and then update score with diversity weight
    val recWithDiversity = userRecsWithBatchRecs.map(userRec => userRec).map(userRec => userRec)
    
    //sort recs for a user by rec score and take first 30
    val sortedRec = recWithDiversity.map(userRec => (userRec._1, userRec._2.sortBy(rec => rec._2).take(30)))
    
    //save recs for each user to cassandra
    sortedRec.foreach(event=>{
      val rddSize = event.count()
      if(rddSize > 0){
        event.saveAsTextFile(ConfigLoader.query("streaming_validation_file_path_toConstruct") + event.id)       
      }
    })
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(10 * 1000) //10 sec
  }
}