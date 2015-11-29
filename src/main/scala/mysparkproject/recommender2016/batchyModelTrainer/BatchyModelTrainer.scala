package mysparkproject.recommender2016.batchyModelTrainer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import java.nio.file.{Paths, Files}
import java.io._

import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable.ListBuffer

import mysparkproject.recommender2016.util.UnsplittableTextInputFormat
import mysparkproject.recommender2016.util.ConfigLoader

object batchyModelTrainer {
  def main(args: Array[String]) {
    //reading acc ratings
    /*
    val eventFiles = "hdfs://c6501.ambari.apache.org:8020/user/yliu/spark-input/daily_user_track_event_*.txt"
    should be a HDFS path in config file. mocking for now
    */
    var ratingsFilesPath = ConfigLoader.query("accumulated_rating_file_path")
    //overwriting input file location in local test
    if (args.length > 0 && args(0).equals("test")){
      ratingsFilesPath = "/sparkproject/localtest/accumulatedRatings-001*.txt"
    }
    var accFilesToConstruct = ConfigLoader.query("updated_accumulated_rating_file_path_toConstruct")
    //overwriting input file location in local test
    if (args.length > 0 && args(0).equals("test")){
      accFilesToConstruct = "/sparkproject/localtest/accumulatedRatings-new-001-00"
    }
    var dailyRatingFileToConstruct = ConfigLoader.query("daily_rating_file_path_toConstruct")
    //overwriting input file location in local test
    if (args.length > 0 && args(0).equals("test")){
      dailyRatingFileToConstruct = "/sparkproject/localtest/daily_user_track_event_00"
    }
    val conf = new SparkConf().setAppName("Batch Model Trainer")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ratingsLines = sc.hadoopFile(ratingsFilesPath, 
        classOf[UnsplittableTextInputFormat], 
        classOf[LongWritable],  
        classOf[Text], 4)
        .map(_._2.toString())
    //val ratingsLines = sc.textFile(ratingsFiles,4) //num of partitions should be determined by inputformat

    //parse acc ratings into tuple
    val ratingsTuples = ratingsLines.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, ratings: String) =>
        (userid, trackid, timestamp.toLong, ratings.toDouble)
      //neglecting checking for fault tolerant boundary marker
    })
    //mock a user id to make the rating's user id within this partition's user id range
    val userIdRightEventTuples = ratingsTuples.mapPartitionsWithIndex((index, itr) => {
      val rangeBottom = index * 60000000 / 10 //60M/10
      val randomGenerator = scala.util.Random
      
      val newItr = itr.map(next => {
        val randomUserId = ((index * 60000000 / 10).toLong + randomGenerator.nextDouble * 60000000 / 10).toLong.toString
        (randomUserId, (randomUserId.toLong/2).toString(),//just use randomUserId/2 as track id
            next._3,next._4)
      })
      newItr
    }, true)
    //filter ratings three years ago
    //filter tracks listened less than 10 times and not active within 6 months.
    //filter users not so active
    //need to visit external DB
    val ratingsWithinThreeYears = userIdRightEventTuples.filter { rating => rating._3 > 0 } //mock 3 years here
      .filter(rating => rating._3 > 0) //mock 10 times and 6 months
      .groupBy(rating => rating._1)//should we do on scala level, but not RDD level(to prevent shuffle scheduling)?
    //ratingsWithinThreeYears in the form of Map(userid -> list of ratings)

    //build a map from user+track to rating in memory from acc ratings 
    //we read records in daily rating files one by one. as we read one, calculate the event's score and update the map
    //if user+track doesn't exist yet, insert; otherwise, add current event score to the existing score
    val ratingsWithDailyRating = ratingsWithinThreeYears.mapPartitionsWithIndex((index, itr) =>{
      //maintain a map of userId+trackId -> (time,rating)
      val accMap:scala.collection.mutable.Map[String, (Long,Double)] = scala.collection.mutable.Map()
      var element = ("", Iterable[(String,String,Long,Double)]())
      while(itr.hasNext){
        element = itr.next
        for(pair <- element._2){
          //compose composite key: user id + track id
          val key = pair._1 + "," + pair._2
          //println("rating merging. building map: " + key + ";" +  (pair._3, pair._4))
          accMap.put(key,(pair._3, pair._4))
        }
      }
      println("rating merging. map built: " + accMap.toString())
      
      //mock hashing to get corresponding daily rating file, which contains random user id.
      //mocking user id in daily rating file below. in real case, we use hashing instead of range.
      val rangeBase = 60000000 / 10 //60M/10
      val partitionFileId = 1 + element._1.toInt / rangeBase
      //mock file name. name should be fetched from config file and contain date
      val partitionFileName = dailyRatingFileToConstruct + partitionFileId + ".txt"
      // val partitionFileName = "/Users/yliu/deployment/recommendationProject/daily_user_track_event_001.txt"
      println("rating merging. reading daily event partition file at: " + partitionFileName)
      val rangeBottom = index * 60000000 / 10 //60M/10
      val randomGenerator = scala.util.Random
      if(Files.exists(Paths.get(partitionFileName))){
        for (line <- Source.fromFile(partitionFileName).getLines()) {
          val event = line.split(",") match {
            case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
              (userid, trackid, timestamp.toLong, msPlayed, reasonStart, reasonEnd)
            //neglecting checking for fault tolerant boundary marker
          }
          val randomUserId = rangeBottom + randomGenerator.nextInt(60000000/10)
          val newItem = (randomUserId.toString,event._2,event._3,event._4,event._5,event._6)
          //mock calculating score
          val thisEventScore = randomGenerator.nextDouble() * 5
          //neglect filter tracks listened less than 10 times or not active within 6 months
          //insert into map
          val key = newItem._1+","+newItem._2
          val previousScoreAndTime = accMap.getOrElse(key, (0,0D))
          accMap.put(key, (newItem._3, previousScoreAndTime._2 + thisEventScore))
          //println("rating merging. " + key + " current score: " + previousScoreAndTime + " new record: " + newItem + " new score:" + (newItem._3, previousScoreAndTime._2 + thisEventScore))
        }
      }
      accMap.iterator
    }
    , true)
        
    //in the form of userid -> (list of acc ratings, list of daily events)
    //ratingsWithDailyRating.foreach { case (k, v) => println("rating merging. key: " + k + " value: " + v.toString()) }

    //filter tracks listened less than 10 times or not active within 6 months. visit external DB
    val activeRatings = ratingsWithDailyRating.filter(rating => rating._1 != "") //mock 10 & 6 here

    //convert rating map entry into Rating objects
    def convertToRating(rating: (String, (Long, Double))) = {
      val userId = rating._1.split(",")(0)
      val trackId = rating._1.split(",")(1)
      Rating(userId.toInt, trackId.toInt, rating._2._2)
    }
    
    //write back to acc rating files. for fault tolerant purpose, we write to temp files and then rename
    activeRatings.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val justToRunAJob = activeRatings.mapPartitionsWithIndex((index, ratingItr) => {
      //in real world, file name should based on hash value of userid
      val writer = new PrintWriter(new File(accFilesToConstruct + index + ".txt"))
      while(ratingItr.hasNext){
        val rating = ratingItr.next()
        writer.append(rating._1 + "," + rating._2._1 + "," + rating._2._2 + "\n")
      }
      writer.close()
      "not need for return".iterator
    }
    , true)
    justToRunAJob.count()//just to trigger the writing above
    
    val eventRatings = activeRatings.map(rating => convertToRating(rating))

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
    
    
    //neglecting saving vectors to sparkey and ANNOY files
  }
}