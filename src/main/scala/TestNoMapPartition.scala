import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable.ListBuffer

import mysparkproject.recommender2016.util.ConfigLoader
import mysparkproject.recommender2016.util.UnsplittableTextInputFormat

object TestNoMapPartition {
  
  def main(args: Array[String]) {
    var eventFilesPath = ConfigLoader.query("daily_rating_file_path")
    val conf = new SparkConf().setAppName("TestNoMapPartition")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ratingsLines = sc.hadoopFile(eventFilesPath, 
        classOf[UnsplittableTextInputFormat], 
        classOf[LongWritable],  
        classOf[Text], 4)
        .map(_._2.toString())
    val firstMapRDD = ratingsLines.map { x => {
      val y = x + x
      x
    } }
    /* this is the wrong way
    val mapPartitionRDD = firstMapRDD.mapPartitions(itr => {
      val fruits = new ListBuffer[String]
      while(itr.hasNext){
        val thisEle = itr.next()
        println("mapPartition ele: " + thisEle)
        fruits += thisEle
      }
      fruits.iterator
    }, true)
    */
    val mapPartitionRDD = firstMapRDD.mapPartitions(itr => {
      val newItr = itr.map { x => {
        println("mapPartition ele: " + x)
        x
      } }
      newItr
    }, true)
    val secondMapRDD = mapPartitionRDD.map { x => {
      val y = x + x
      x
    } }
    secondMapRDD.foreach { x => {
      val y = x + x
    } }
  }
}