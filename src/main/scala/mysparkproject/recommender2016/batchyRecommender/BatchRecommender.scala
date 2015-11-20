package mysparkproject.recommender2016.batchyRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable.ListBuffer

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
      val rangeBottom = index * 60000000 / 10 //60M/10
      val randomGenerator = scala.util.Random
      val randomUserId = rangeBottom + randomGenerator.nextInt(60000000/10)
      val fruits = new ListBuffer[(String, String, String, String, String, String)]()
      while(itr.hasNext){
        val next = itr.next()
        val newItem = (randomUserId.toString,next._2,next._3,next._4,next._5,next._6)
        fruits += newItem
      }
      fruits.iterator
    }, true)
    //calculate weight
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
    //fetching item vector for the item
    val eventWithVectorAndWeight = eventWithWeight.map(event =>
      //user id, track id, weight, item vectors
      (event._1, event._2, event._3, mockTrackVector(event._2)))
    /*//validation of vector fetching
    val finalresult = eventWithVectorAndWeight.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3 + ',' + ele._4))
    */
    def mockRecVector() = {
      //since we don't hae ANNOY, we mock rec vectors from it
      val randomGenerator = scala.util.Random
      //generate a random vector first, passing random vec ID
      val recVector = mockTrackVector(randomGenerator.nextInt(100).toString())
      //generate a cosine similarity score in the end, and return the whole vector
      (recVector._1, recVector._2, recVector._3, recVector._4, recVector._5, randomGenerator.nextInt(100))
    }
    val eventWithRecVectorAndWeight = eventWithVectorAndWeight.map(event =>
      //replace item vector for the event with rec vectors for the item
      //user id, track id, weight, 5 rec vectors
      (event._1, event._2, event._3, mockRecVector, mockRecVector, mockRecVector, mockRecVector, mockRecVector))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    /*//validation of rec vector fetching
    val finalresult = eventWithRecVectorAndWeight.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3 + ',' + ele._4+ ',' + ele._5+ ',' + ele._6+ ',' + ele._7+ ',' + ele._8))
    */
    //calculate score with weight and cosine similarity for each rec vector
    def calculateRecVectorScore(weight: Double, vec: (String, Int, Int, Int, Int, Int)) = {
      //we don't need the vector anymore, just the track id and the weight
      (vec._1, vec._6 * weight)
    }
    val trackWithRecVectorCalculated = eventWithRecVectorAndWeight.map(event =>
      (event._1, event._2, calculateRecVectorScore(event._3, event._4), calculateRecVectorScore(event._3, event._5), calculateRecVectorScore(event._3, event._6), calculateRecVectorScore(event._3, event._7), calculateRecVectorScore(event._3, event._8)))
    /*//validation of calculating score
    val finalresult = trackWithRecVectorCalculated.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ',' + ele._2 + ',' + ele._3 + ',' + ele._4 + ',' + ele._5 + ',' + ele._6 + ',' + ele._7))
		*/
    //mock bloom filter
    val trackWithRecVectorFiltered = trackWithRecVectorCalculated.filter(element => 2 == 2)
    //group rec tracks for each user
    //val userAllRecTracks = trackWithRecVectorFiltered.groupBy(_._1)
    val userAllRecTracks = trackWithRecVectorFiltered.mapPartitions(itr => {
      val fruits = new ListBuffer[(String, String, (String, Double), (String, Double), (String, Double), (String, Double), (String, Double))]()
      while(itr.hasNext){
        fruits += itr.next()
      }
      val groupedby = fruits.groupBy(_._1)
      groupedby.iterator
    }, true)
    /*
    //validation of groupBy
    val finalresult = userAllRecTracks.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele))
    */
    
    // within each user's data record, rank rec tracksby rec score.
    // start from extracting a list of (user, track, rec1, rec2,...)
    val perUserRecs = userAllRecTracks.map(userTracks => {
        //get the list of (user, track, rec1, rec2,...) belonging to each user
        val userTrackList = userTracks._2.toList
        //extracting rec from each (user, track, rec1, rec2,...)
        val AllSortedRecUserTrackList = userTrackList.map(trackRecs => {
            //recTrackList is all recs from one event; trackRecs is one event
            List(trackRecs._3, trackRecs._4, trackRecs._5, trackRecs._6, trackRecs._7)
        }).flatMap { trackRecs => 
          //splitting recs for one track into recs and aggregate all tracks' recs into a list
          trackRecs.iterator }
        //sort among all recs from all tracks according to their score
        .sortBy(_._2)
        
        //should not merge by track id here because trackId with higher score means it should be reced because of some track
        //return result (userId, List((trackId, score),(trackId, score),...))
        (userTracks._1, AllSortedRecUserTrackList)
      }
    )
    //validation
    println("finalresult:")
    val finalresult = perUserRecs.collect()
    finalresult.foreach(ele => println(ele))
    
    //mocking filtering track dismissed by user
    //neglecting compute diversity factor
    //neglecting top30 per user
		
    //storing the value to cassandra table
    
  }
}