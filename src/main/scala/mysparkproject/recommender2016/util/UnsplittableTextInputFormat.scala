package mysparkproject.recommender2016.util

//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.TextInputFormat

class UnsplittableTextInputFormat extends TextInputFormat{
  
  @Override
  protected def	isSplitable(context:JobContext, file: Path):Boolean = {
    false
  }
}
