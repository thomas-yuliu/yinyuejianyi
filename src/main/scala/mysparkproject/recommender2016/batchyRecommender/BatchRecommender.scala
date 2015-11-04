package mysparkproject.recommender2016.batchyRecommender

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import mysparkproject.recommender2016.util.ConfigLoader

object BatchRecommender {
  
  def main(args: Array[String]) {
    
    //mocking input file on HDFS
    val eventFiles = ConfigLoader.query("daily_rating_path")
    val conf = new SparkConf().setAppName("Batch Recommender")
    val sc = new SparkContext(conf)
    val eventLines = sc.textFile(eventFiles, 4) //in real case, num of partitions determined by inputformat

    //parse daily ratings into tuple
    val eventTuples = eventLines.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
        (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
    })
    /*//validation of file reading and parsing
    val finalresult = eventTuples.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1 + ele._2 + ele._3 + ele._4 + ele._5))
    */
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
    val eventWithWeight = eventTuples.map(calculateWeightForEvent)
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
    val userAllRecTracks = trackWithRecVectorFiltered.groupBy(_._1)
    /*
    //validation of groupBy
    val finalresult = userAllRecTracks.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele))
    */
    
    //rank by rec score of tracks within each user's data record.
    val perUserRecs = userAllRecTracks.map(userTracks => {
        val userTrackList = userTracks._2.toList
        val AllSortedRecUserTrackList = userTrackList.map(trackRecs => {
            val recTrackList = List(trackRecs._3, trackRecs._4, trackRecs._5, trackRecs._6, trackRecs._7)
            val sortedRecForOneTrack = recTrackList.sortBy(_._2)
            sortedRecForOneTrack
          }
        )//now AllSortedRecUserTrackList is ((trackId, score),...,(trackId, score)),((trackId, score),...,(trackId, score)),...
        val flatRecList = AllSortedRecUserTrackList.flatMap(sortedRecForOneTrack => {
          sortedRecForOneTrack.iterator
        })//now flatRecList is (trackId, score),(trackId, score),...
        //should not merge by track id here because trackId with higher score means it should be reced because of some track
        //return result (userId, List((trackId, score),(trackId, score),...))
        (userTracks._1, flatRecList)
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