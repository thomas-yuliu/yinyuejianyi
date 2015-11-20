package mysparkproject.recommender2016.util

//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.TextInputFormat

class UnsplittableTextInputFormat extends TextInputFormat{
  
  override def	isSplitable(fs:FileSystem, file: Path):Boolean = {
    false
  }
}
