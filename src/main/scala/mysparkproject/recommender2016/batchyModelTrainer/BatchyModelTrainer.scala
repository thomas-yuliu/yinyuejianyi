package mysparkproject.recommender2016.batchyModelTrainer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

object batchyModelTrainer {
  def main(args: Array[String]) {
    val eventFiles = "hdfs://c6501.ambari.apache.org:8020/user/yliu/spark-input/daily_user_track_event_*.txt" // configurable
    val conf = new SparkConf().setAppName("Batch Recommender")
    val sc = new SparkContext(conf)
    val eventLines = sc.textFile(eventFiles) //num of partitions determined by inputformat

    //parse into tuple
    val eventTuples = eventLines.map(line => line.split(",") match {
      case Array(userid: String, trackid: String, timestamp: String, msPlayed: String, reasonStart: String, reasonEnd: String) =>
        (userid, trackid, timestamp, msPlayed, reasonStart, reasonEnd)
    })
    
    //mock score calculation process
    def convertToRating(userId:Int, trackId:Int) = {
      val randomGenerator = scala.util.Random//need to be put to partition level
      Rating(userId.toInt, trackId.toInt, randomGenerator.nextDouble()*5)
    }
    val eventRatings = eventTuples.map(event => convertToRating(event._1.toInt, event._2.toInt))
    
    //train the model with ALS
    val rank = 10
    val numIterations = 10
    val lambda = 0.01
    val alpha = 40
    val model = ALS.trainImplicit(eventRatings, rank, numIterations, lambda, alpha)
    
    //fetch track vectors
    val productVectorsRDD = model.productFeatures
    //validation of productVectorsRDD
    val finalresult = productVectorsRDD.collect()
    println("finalresult:")
    finalresult.foreach(ele => println(ele))
  }
}