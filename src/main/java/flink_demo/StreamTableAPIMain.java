package flink_demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.table.GroupedTable;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by philipp on 14.05.16.
 */
public class StreamTableAPIMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WC> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter());


        /*dataStream.timeWindow(Time.seconds(5))
                .sum(1).print();*/

        StreamTableEnvironment streamTableEnvironment = TableEnvironment.getTableEnvironment(env);

        //streamTableEnvironment.registerDataStream("words",dataStream);

        //Table select = streamTableEnvironment.sql("select * from words");
        Table select = streamTableEnvironment.fromDataStream(dataStream).select("word");
        DataStream<String> items = streamTableEnvironment.toDataStream(select,String.class);
        items.print();
        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, WC> {

        public void flatMap(String sentence, Collector<WC> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new WC(word, 1));
            }
        }
    }
}
