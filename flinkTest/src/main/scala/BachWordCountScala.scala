import org.apache.flink.api.scala.ExecutionEnvironment

object BachWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\data\\file"
    val outPath = "D:\\data"
    val env  = ExecutionEnvironment.getExecutionEnvironment
    val text =    env.readTextFile(inputPath)
import org.apache.flink.api.scala._
  val counts =  text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)

    counts.writeAsCsv(outPath,"\n"," ")
    env.execute("batch word count")
  }

}
