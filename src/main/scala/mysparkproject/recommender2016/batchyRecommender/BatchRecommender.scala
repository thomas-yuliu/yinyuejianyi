package mysparkproject.recommender2016.batchyRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable.ListBuffer
import java.io._
import scala.io._

import mysparkproject.recommender2016.util.ConfigLoader
import mysparkproject.recommender2016.util.UnsplittableTextInputFormat

object BatchRecommender {
  
  def main(args: Array[String]) {
    
    //mocking input file on HDFS
    var eventFilesPath = ConfigLoader.query("daily_rating_file_path")
    //overwriting input file location in local test
    if (args.length > 0 && args(0).equals("test")){
      eventFilesPath = "/sparkproject/localtest/daily_user_track_event_*.txt"
    }
    val conf = new SparkConf().setAppName("Batch Recommender")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val eventLines = sc.hadoopFile(eventFilesPath, 
        classOf[UnsplittableTextInputFormat], 
        classOf[LongWritable],  
        classOf[Text], 4)
        .map(_._2.toString())
    //val eventLines = sc.textFile(eventFiles, 4) //in real case, num of partitions determined by inputformat

    //parse daily ratings into tuple
    val eventTuples = eventLines.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
        (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
      //neglecting checking for fault tolerant boundary marker
    })
    /*//validation of file reading and parsing
    val finalresult = eventTuples.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ele._2 + ele._3 + ele._4 + ele._5 + ele._6))
    */
    //mock a user id to make the rating's user id within this partition's user id range
    val userIdRightEventTuples = eventTuples.mapPartitionsWithIndex((index, itr) => {
      val rangeBottom = (index * 60000000 / 10).toLong //60M/10
      val randomGenerator = scala.util.Random
      val randomUserId = rangeBottom + randomGenerator.nextDouble * 60000000 / 10
      
      val resultItr = itr
      .map(next => (randomUserId.toString,next._2,next._3,next._4,next._5,next._6))
      
      resultItr
    }, true)
    //calculate weight for each user event
    def calculateWeightForEvent(event: (String, String, String, String, String, String)) = {
      val reasonStart = event._5
      val reasonWeight = reasonStart match {
        case "query" => 10
        case "rec" => 5
        case "browsing" => 5
        case "radio" => 3
      }
      val msPlayed = event._4.toDouble
      val weight = reasonWeight * msPlayed
      (event._1, event._2, weight)
    }
    val eventWithWeight = userIdRightEventTuples.map(calculateWeightForEvent)
    /*//validation of weight calculation
    val finalresult = eventWithWeight.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3))
    */
    def mockTrackVector(trackId: String) = {
      //we don't have sparkey file and library, just mock a vector of same dimension
      val randomGenerator = scala.util.Random //need to be moved to partition level
      val dimension1 = randomGenerator.nextInt(100)
      val dimension2 = randomGenerator.nextInt(100)
      val dimension3 = randomGenerator.nextInt(100)
      val dimension4 = randomGenerator.nextInt(100)
      (trackId, dimension1, dimension2, dimension3, dimension4)
    }
    //mock fetching item vector for the track from Sparkey
    val eventWithVectorAndWeight = eventWithWeight.map(event =>
      //user id, track id, weight, item vectors
      (event._1, event._2, event._3, mockTrackVector(event._2)))
    /*//validation of vector fetching
    val finalresult = eventWithVectorAndWeight.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3 + ',' + ele._4))
    */
    //groupby user within the partition
    val groupedbyUser = eventWithVectorAndWeight.mapPartitions(itr => {
      val fruits = itr.toList
      val groupedby = fruits.groupBy(_._1)
      groupedby.iterator
    }, true)
    //fetch rec for each event from ANNOY and maintain top 30
    val top30RecPerUser = groupedbyUser.map( userAndTracks => {
      //fetch list of tracks/events for this user
      val tracks = userAndTracks._2
      var top30 = ListBuffer[(String,Double)]()
      val justToPreventBeingReturned = tracks.foreach(track => {
        //mock fetching rec tracks for this track from ANNOY
        val recForThisTrack = ListBuffer[(String, Int, Int, Int, Int, Int)]()
        
        def mockOneRecVector() = {
          //since we don't hae ANNOY, we mock rec vectors from it
          val randomGenerator = scala.util.Random
          //generate a random vector first, passing random vec ID
          val recVector = mockTrackVector(randomGenerator.nextInt(100).toString())
          //generate a cosine similarity score in the end, and return the whole vector
          (recVector._1, recVector._2, recVector._3, recVector._4, recVector._5, randomGenerator.nextInt(100))
        }
        
        for(a <- 1 to 10){
          val curr = mockOneRecVector()
          recForThisTrack+=curr
        }
        //mock filtering through bloom filter
        val afterBloomFiler = recForThisTrack.filter(element => {
          //mock reading bloom filter file. just to leave some latency here
          val bloomfilterFilesPath = "/sparkproject/config/recommenderConfig.json"
          for (line <- Source.fromFile(bloomfilterFilesPath).getLines) {
            println(line)
           }
          2 == 2
        })
        //mock filtering through dismiss filter
        val afterDismissFiler = afterBloomFiler.filter(element => {
          //mock reading bloom filter file. just to leave some latency here
          val bloomfilterFilesPath = "/sparkproject/config/recommenderConfig.json"
          for (line <- Source.fromFile(bloomfilterFilesPath).getLines) {
            println(line)
           }
          2 == 2
        })
        //mock calculating rec score with weight and cosine similarity
        val afterCalculatedRecScore = afterDismissFiler.map(recVector => {
          //don't need anything but the track id and the rec score to reduce memory consumption
          (recVector._1, recVector._6 * track._3)//track._3 is weight
        })
        top30 ++= afterCalculatedRecScore
        //only need the highest 30
        top30 = top30.sortBy(_._2).take(30)
      //end of foreach
      })
      (userAndTracks._1,top30)
    //end of map
    }).persist(StorageLevel.DISK_ONLY_2)
    
    //mock fetch previous day's batch rec from cassandra
    val mergedWithBatchRec = top30RecPerUser.map( userAndRecs =>{
      val userId = userAndRecs._1
      val recs = userAndRecs._2
      //mock fetching from cassandra. just to leave some latency here
      val bloomfilterFilesPath = "/sparkproject/config/recommenderConfig.json"
      recs ++= recs
      for (line <- Source.fromFile(bloomfilterFilesPath).getLines) {
        println(line)
      }
      
      //mock filtering through bloom filter. just to leave some latency here
      for (line <- Source.fromFile(bloomfilterFilesPath).getLines) {
        println(line)
      }
      //mock filtering through dismiss filter. just to leave some latency here
      for (line <- Source.fromFile(bloomfilterFilesPath).getLines) {
        println(line)
      }
      (userId, recs)
    })
    
    val finalResult = mergedWithBatchRec
    //mock calculating diversity score
    .map { userAndRecs =>{
      val userId = userAndRecs._1
      val recs = userAndRecs._2
      //mock reading diversity factor from DB. just to leave some latency here
      val bloomfilterFilesPath = "/sparkproject/config/recommenderConfig.json"
      for (line <- Source.fromFile(bloomfilterFilesPath).getLines) {
        println(line)
      }
      //mock calculating diversity score
      val randomGenerator = scala.util.Random
      val updated = recs.map(rec => {
        (rec._1, rec._2 * randomGenerator.nextDouble())
      })
      (userId, updated)
    } }
    //take top 30 for each user
    .map(userAndRecs =>{
      val userId = userAndRecs._1
      val recs = userAndRecs._2
      val top30 = recs.sortBy(_._2)//_2 is rec score
      .take(30)
      (userId, top30)
    }
    )
    
    //mock storing the value to cassandra table
    finalResult.foreachPartition(itr =>{
      println("finalresult: ")
      var validation_result = ConfigLoader.query("validation_result")
      //overwriting input file location in local test
      if (args.length > 0 && args(0).equals("test")){
        validation_result = "/sparkproject/localtest/result.txt"
      }
      val file = new File(validation_result)
      val bw = new BufferedWriter(new FileWriter(file))
      itr.foreach(each => {
        bw.write(each.toString())
        if(!itr.hasNext){
          bw.close()
        }
      })
    })
  }
}