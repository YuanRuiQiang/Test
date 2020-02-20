import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class JavaTest {

    public static void main(String[] args) throws Exception {
        int port;
       try {
           ParameterTool parameterTool = ParameterTool.fromArgs(args);
           port = parameterTool.getInt("port");
       }catch (Exception e){
           System.err.println("No port specified. use default 9000 -java");
           port = 9000;
       }


        StreamExecutionEnvironment  env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "hadoop101";
        String delimiter = "\n";
        DataStreamSource<String> text = env.socketTextStream(hostname,port,delimiter);

        SingleOutputStreamOperator<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //指定时间窗口大小为2秒，指定时间间隔为1秒
                .sum("count");
        //or reduce
        //打印至控制台 设置行度
        windowCounts.print().setParallelism(1);
        //必须实现否则不执行
        env.execute("Socket window count");
    }
    public static  class WordWithCount{
        public String word;
        public  long count;
        public  WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString(){
          return  "WordWithCount{" +
                  "word='" + word + '\'' +
                  ",count=" + count +
                  '}';
        }
    }
}
