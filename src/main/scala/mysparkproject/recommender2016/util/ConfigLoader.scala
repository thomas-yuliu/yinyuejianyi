package mysparkproject.recommender2016.util

import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

object ConfigLoader {
  val installation_path = sys.env("INSTALL_LOCATION")
  println("")
  println("")
  println("")
  println("system env: " + sys.env.toString())
  println("")
  println("")
  println("")
  println("")
  var jsonObject = loadInputConfigFile(installation_path + "/config/recommenderConfig.json")
  
  def loadInputConfigFile(inputFilePath:String):JSONObject = {
    val parser = new JSONParser();
    var jsonObject:JSONObject = null;
  
    try {
      jsonObject = parser.parse(new FileReader(inputFilePath)).asInstanceOf[JSONObject]
    } catch {
      case ioe: IOException => println("IOException: " + ioe)
      case pe: ParseException => println("ParseException: " + pe)
    }
    jsonObject
  }
  
  def query(key:String):String = {
    jsonObject.get(key).toString()
  }
}