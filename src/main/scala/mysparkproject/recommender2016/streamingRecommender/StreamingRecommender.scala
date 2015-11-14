package mysparkproject.recommender2016.streamingRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer

import mysparkproject.recommender2016.util.ConfigLoader

object StreamingRecommender {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Recommender")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //val ssc = new StreamingContext(conf, Seconds(5)) //will be per minute in real case
    
    //in real case should be HDFS path, it seems to work now
    val checkpoint_dir = "/sparkproject/streaming_checkpoint"
    def creatingFunc() : StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(10)) //will be per minute in real case
      ssc.checkpoint(checkpoint_dir)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpoint_dir, creatingFunc)
    //mock broadcasting user vector: XXT
    val mockedXXT = new ListBuffer[String]()
    for(a <- 1 to 1000){
       mockedXXT ++= Seq("variable")
    }
    val mockedBCVariable = ssc.sparkContext.broadcast(mockedXXT)
    
    val zkQuorum = "localhost"
    val consumerGroup = "streamingRecommender"
    val topicName = ConfigLoader.query("daily_rating_kafka_topic")
    val topicMap = collection.immutable.HashMap(topicName -> 1)

    val msgs = KafkaUtils.createStream(ssc, zkQuorum, consumerGroup, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    //msgs.saveAsTextFiles("/Users/yliu/deployment/recommendationProject/streamingResult/sresult", "txt")

    //parse daily events into tuple
    val eventPairs = msgs.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
        trackid -> (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
    })
    
    val groupByTrack = eventPairs.map(event => event._1 -> List(event._2)) //convert to list to make reduce function easier
      .reduceByKey((event1, event2) => event1 ++ event2)//could be a long list for hot songs
    //now in the form of trackId -> list(event)
    val computedWeight = groupByTrack.map {
      case (trackId, events) => {
        //mock score computation
        val randomGenerator = scala.util.Random
        val eventScores = events.map(event => (event._1, randomGenerator.nextInt(10)))
        trackId -> eventScores
      }
    }

    //mock fetching rec vector from Sparkey
    val fetchedTrackVector = computedWeight.map { trackJson => (trackJson._1, (0.1, 0.1, 0.1), trackJson._2)}
    
    //mock updating track vector with ML ALS, using data needed for training ALS in DB
    val updatedVector = fetchedTrackVector.map(trackJson => (trackJson._1, (0.1, 0.1, 0.1), trackJson._3))

    //mock fetching rec tracks for this track from ANNOY
    val findRecVector = updatedVector.map(trackJson => (trackJson._1, List((0.1, 0.1, 0.1),(0.2, 0.2, 0.2),(0.3, 0.3, 0.3)), trackJson._3)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    //only to demo broadcasted variables are accessible from workers
    /*val nothing = findRecVector.map(rec => {
      println("I found broadcast variable" + mockedBCVariable.value)
      rec
    })
    nothing.foreach(record => {
      record.saveAsTextFile("/Users/yliu/nothing/ok")
    })*/
    
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
    
    //neglecting saving recs to Cassandra
    
    ssc.start()
    ssc.awaitTermination()
    //ssc.awaitTerminationOrTimeout(10 * 1000) //10 sec
  }
}