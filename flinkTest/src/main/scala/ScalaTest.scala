
import org.apache.flink.api.java.utils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
object ScalaTest {
  def main(args: Array[String]): Unit = {

    val port:Int = try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch{
      case e:Exception => {
        System.err.println("No port specified. use default 9000 -scala")
      }
        9000
    }

    val env:StreamExecutionEnvironment  = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("hadoop101",port,'\n')
  import org.apache.flink.api.scala._
    val windowCounts = text.flatMap(w=>w.split("\\s"))
      .map(w=> WordWithCount(w,1))
      .keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")
    windowCounts.print().setParallelism(1)
    env.execute("Socket window count")
  }

  case class WordWithCount(word:String,count: Long)
}
