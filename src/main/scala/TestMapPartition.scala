import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable.ListBuffer

import mysparkproject.recommender2016.util.ConfigLoader
import mysparkproject.recommender2016.util.UnsplittableTextInputFormat

object TestMapPartition {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestMapPartition")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val firstRDD = sc.textFile("/sparkproject/testMapPartition.txt", 4)
    val firstMapRDD = firstRDD.map { x => {
      println("first map ele: " + x)
      x
    } }
    val mapPartitionRDD = firstMapRDD.mapPartitions(itr => {
      val fruits = new ListBuffer[String]
      while(itr.hasNext){
        val thisEle = itr.next()
        println("mapPartition ele: " + thisEle)
        fruits += thisEle
      }
      fruits.iterator
    }, true)
    val secondMapRDD = mapPartitionRDD.map { x => {
      println("second map ele: " + x)
      x
    } }
    secondMapRDD.foreach { x => {
      println("foreach ele: " + x)
    } }
  }
}