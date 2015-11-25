package mysparkproject.recommender2016.batchyRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
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
      val randomUserId = (rangeBottom + randomGenerator.nextDouble * 60000000 / 10).toLong
      //need to put to long to get rid of decimals
      val resultItr = itr
      .map(next => (randomUserId.toString,next._2,next._3,next._4,next._5,next._6))
      
      resultItr
    }, true)
    
    //progress logs
    val userIdRightEventTuplesB = userIdRightEventTuples.mapPartitionsWithIndex((partitionId,itr) => {
      println("progress: user id generated in partition " + partitionId)
      itr
    }, true)
    
    //calculate weight for each user event
    def calculateWeightForEvent(event: (String, String, String, String, String, String)) = {
      /*val reasonStart = event._5
      val reasonWeight = reasonStart match {
        case "query" => 10
        case "rec" => 5
        case "browsing" => 5
        case "radio" => 3
      }
      val msPlayed = event._4.toDouble
      val weight = reasonWeight * msPlayed*/
      val weight = event._1.toLong / 6000000
      (event._1, event._2, weight.toDouble)
    }
    
    val eventWithWeight = userIdRightEventTuplesB.map(calculateWeightForEvent)
    /*//validation of weight calculation
    val finalresult = eventWithWeight.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3))
    */
    
    //progress logs
    val eventWithWeightB = eventWithWeight.mapPartitionsWithIndex((partitionId,itr) => {
      println("progress: weight calculated in partition " + partitionId)
      itr
    }, true)
    
    def mockTrackVector(trackId: String) = {
      //we don't have sparkey file and library, just mock a vector of same dimension
      /*val randomGenerator = scala.util.Random //need to be moved to partition level
      val dimension1 = randomGenerator.nextInt(100)
      val dimension2 = randomGenerator.nextInt(100)
      val dimension3 = randomGenerator.nextInt(100)
      val dimension4 = randomGenerator.nextInt(100)*/
      val dimension1 = trackId.toLong / 277
      val dimension2 = trackId.toLong / 483
      val dimension3 = trackId.toLong / 399
      val dimension4 = trackId.toLong / 571
      (trackId, dimension1.toInt, dimension2.toInt, dimension3.toInt, dimension4.toInt)
    }
    /*
    //mock fetching item vector for the track from Sparkey
     * for now this is moved after groupby
    val eventWithVectorAndWeight = eventWithWeightB.map(event =>
      //user id, track id, weight, item vectors
      (event._1, event._2, event._3, mockTrackVector(event._2)))
      * */
      
    /*//validation of vector fetching
    val finalresult = eventWithVectorAndWeight.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3 + ',' + ele._4))
    */
    //groupby user within the partition
    /*
    val groupedbyUser = eventWithWeightB.mapPartitions(itr => {
      var fruits = itr.toArray
      val groupedby = fruits.groupBy(_._1)
      fruits = Array()
      groupedby.iterator
      //val map = Map[String, ListBuffer[(String, String, Double, (String, Int, Int, Int, Int))]]()
      //val newitr = itr.map(item => {
        //Array[(String, String, Double, (String, Int, Int, Int, Int))]
        //(item._1, Array((item._1, item._2, 1.0, (item._2,1,1,1,1))))
      //})
      //newitr
    }, true)
    
    //progress logs
    val groupedbyUserB = groupedbyUser.mapPartitionsWithIndex((partitionId,itr) => {
      println("progress: groupedby for each user in partition " + partitionId)
      itr
    }, true)
    */
    //just for latency
    val installation_path = sys.env("INSTALL_LOCATION")
    var targetfilepath = installation_path + "/config/recommenderConfig.json"
  
    def mockLatency(length: Int) = {
       //Thread sleep length
      val file = new File(targetfilepath)
      val bw = new BufferedReader(new FileReader(file))
      for(a <- 1 to length){
       val line = bw.readLine()
      }
      bw.close()
    }
    
    val top20RecPerUser = eventWithWeightB.mapPartitions(itr => {
      
      val top20 = Map[String, Array[(String,Double)]]()
      
      while(itr.hasNext){
        val event = itr.next()
        
        //mock fetching track vector for this track from Sparkey
        val mockVector = mockTrackVector(event._2)
        
        //mock fetching rec tracks for this track from ANNOY
        var recForThisTrack = new Array[(String, Int, Int, Int, Int, Int)](10)
        
        def mockOneRecVector(randomNum: String) = {
          //since we don't hae ANNOY, we mock rec vectors from it
          //val randomGenerator = scala.util.Random
          //generate a random vector first, passing random vec ID
          //val recVector = mockTrackVector(randomGenerator.nextInt(100).toString())
          val recVector = mockTrackVector(randomNum)
          //generate a cosine similarity score in the end, and return the whole vector
          (recVector._1, recVector._2, recVector._3, recVector._4, recVector._5, (randomNum.toLong/103).toInt)
        }
        
        //mock ANNOY latency
        mockLatency(20)
        for(a <- 0 to 9){
          recForThisTrack(a) = mockOneRecVector(event._2)  
        }
        //mock filtering through bloom filter and dismiss filter
        var afterDismissFiler = recForThisTrack.filter(element => {
          //caching bloom filter and dismiss in memory. latency is neglectable
          //hard to find a latency below 1ms so put the latency up in ANNOY
          2 == 2
        })
        //mock calculating rec score with weight and cosine similarity
        var afterCalculatedRecScore = afterDismissFiler.map(recVector => {
          //don't need anything but the track id and the rec score to reduce memory consumption
          (recVector._1, recVector._6 * event._3.toDouble)//track._3 is weight
        })
        if(top20.contains(event._1)){
          val existing = top20.get(event._1).get
          val newArray = afterCalculatedRecScore ++ existing
          val sortedArray = newArray.sortBy(_._2).take(30)
          top20.put(event._1, sortedArray)
        } else {
          top20.put(event._1, afterCalculatedRecScore)
        }
      }
      
      println("top20 for users " + top20.toString())
      
      top20.iterator
    }, true).persist(StorageLevel.DISK_ONLY_2)
    
    //progress logs
    val top20RecPerUserB = top20RecPerUser.mapPartitionsWithIndex((partitionId,itr) => {
      println("progress: top20 built for each user in partition " + partitionId)
      itr
    }, true)
    
    //mock fetch previous day's batch rec from cassandra
    val mergedWithBatchRec = top20RecPerUserB.map( userAndRecs =>{
      val userId = userAndRecs._1
      val recs = userAndRecs._2
      //mock fetching from cassandra
      val allrecs = recs ++ recs
      //mock filtering through bloom filter and dismiss filter
      //mockLatency(1)
      //caching bloom filter and dismiss in memory. latency is neglectable
      val afterfiltering = allrecs.filter(ele => 2==2)
      //hard to find a latency below 1ms so put the latency up in ANNOY
      (userId, afterfiltering)
    })
    
    //progress logs
    val mergedWithBatchRecB = mergedWithBatchRec.mapPartitionsWithIndex((partitionId,itr) => {
      println("progress: today's recs merged with previous recs in partition " + partitionId)
      itr
    }, true)
    
    val finalResult = mergedWithBatchRecB
    //mock calculating diversity score
    .map { userAndRecs =>{
      val userId = userAndRecs._1
      val recs = userAndRecs._2
      //caching bloom filter and dismiss in memory. latency is neglectable
      //hard to find a latency below 1ms so put the latency up in ANNOY
      //mock calculating diversity score
      val updated = recs.map(rec => {
        (rec._1, (userId.toLong/rec._2).toDouble)
      })
      (userId, updated)
    } }
    //take top 30 for each user
    .map(userAndRecs =>{
      val userId = userAndRecs._1
      val recs = userAndRecs._2
      val top20 = recs.sortBy(_._2)//_2 is rec score
      .take(20)
      (userId, top20)
    }
    )
    
    //mock storing the value to cassandra table
    val appId = sc.applicationId
    var validation_result = ConfigLoader.query("validation_result")
    //overwriting input file location in local test
    if (args.length > 0 && args(0).equals("test")){
      validation_result = "/sparkproject/localtest/result"
    }
    validation_result = validation_result + appId
    validation_result += ".txt"
    finalResult.foreachPartition(itr =>{
      println("finalresult: ")
      val file = new File(validation_result)
      val bw = new BufferedWriter(new FileWriter(file, true))
      while(itr.hasNext){
        bw.write(itr.next()._1 + " recs: " + itr.next()._2.toString() + "\n")
      }
      bw.close()
      /*itr.foreach(each => {
        bw.write(each.toString())
        if(!itr.hasNext){
          bw.close()
        }
      })*/
    })
  }
}