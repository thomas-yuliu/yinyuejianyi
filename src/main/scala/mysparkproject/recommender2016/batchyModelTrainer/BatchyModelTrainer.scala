package mysparkproject.recommender2016.batchyModelTrainer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import scala.io.Source

object batchyModelTrainer {
  def main(args: Array[String]) {
    //reading acc ratings
    /*
    val eventFiles = "hdfs://c6501.ambari.apache.org:8020/user/yliu/spark-input/daily_user_track_event_*.txt"
    should be a HDFS path in config file. mocking for now
    */
    val ratingsFiles = "/Users/yliu/deployment/recommendationProject/accumulatedRatings*"
    val conf = new SparkConf().setAppName("Batch Model Trainer")
    val sc = new SparkContext(conf)
    val ratingsLines = sc.textFile(ratingsFiles,4) //num of partitions should be determined by inputformat

    //parse acc ratings into tuple
    val ratingsTuples = ratingsLines.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, ratings: String) =>
        (userid, trackid, timestamp.toLong, ratings)
    })

    //filter ratings three years ago, tracks listened less than 10 times and not active within 6 months. visit external DB
    val ratingsWithinThreeYears = ratingsTuples.filter { rating => rating._3 > 0 } //mock 3 years here
      .filter(rating => rating._3 > 0) //mock 10 times and 6 months
      .groupBy(rating => rating._1)
    //ratingsWithinThreeYears in the form of Map(userid -> list of ratings)

    //read in daily ratings for each user
    /*
     * is it possible to read one file once per partition. note don't get out of memory
     * choice A: split users in the partition into 5 groups
     * 						instead of reading the file once for each user, read once for each group
     * 						file size may not be around average, may go to say 8GB. 
     * 						we can choose diff algorithms according to diff file size
     * 
     * mapPartition. if RDD can't fill in memory, will surely have out of memory exception? maybe not, we are using iterators
     */
    def readDailyRatingFromUserId(userId: String) = {
      //mock hashing
      val partitionId = 1 + userId.toInt % 4
      //mock file name. name should be fetched from config file and contain date
      val partitionFileName = "/Users/yliu/deployment/recommendationProject/daily_user_track_event_00" + partitionId + ".txt"
      // val partitionFileName = "/Users/yliu/deployment/recommendationProject/daily_user_track_event_001.txt"
      println("trying to read daily event partition file at: " + partitionFileName)
      var events = List[(String, String, String, String, String, String)]()
      for (line <- Source.fromFile(partitionFileName).getLines()) {
        val event = line.split(",") match {
          case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
            (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
        }
        if (event._1 == userId) {
          events ::= event
        }
      }
      events
    }
    val ratingsWithDailyRating = ratingsWithinThreeYears.map(rating => rating._1 -> (rating._2, readDailyRatingFromUserId(rating._1)))
    //in the form of userid -> (list of acc ratings, list of daily events)
    ratingsWithDailyRating.foreach { case (k, v) => println("key: " + k + " value: " + v.toString()) }

    //filter tracks listened less than 10 times or not active within 6 months. visit external DB
    val activeRatings = ratingsWithDailyRating.filter(rating => rating._1 != "") //mock 10 & 6 here

    //mock merge ratings by user id & track id and score calculation process
    def convertToRating(userId: Int, trackId: Int) = {
      val randomGenerator = scala.util.Random //need to be put to partition level
      Rating(userId.toInt, trackId.toInt, randomGenerator.nextDouble() * 5)
    }
    val eventRatings = ratingsTuples.map(event => convertToRating(event._1.toInt, event._2.toInt))

    //train the model with ALS
    //those will be read from config files
    val rank = 10
    val numIterations = 20
    val lambda = 0.01
    val alpha = 40
    val model = ALS.trainImplicit(eventRatings, rank, numIterations, lambda, alpha)

    //fetch track vectors
    val productVectorsRDD = model.productFeatures
    //validation of productVectorsRDD
    val finalresult = productVectorsRDD.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele._1, ele._2.mkString(" ")))
  }
}