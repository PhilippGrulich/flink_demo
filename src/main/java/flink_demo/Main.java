package flink_demo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;

import java.util.List;

/**
 * Created by philipp on 12.05.16.
 * A Simple Flink Table API Demo
 */
public class Main {

    public static void main(String[] args){

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
        DataSource<WC> input = env.fromElements(new WC("hello", 1), new WC("hello", 1), new WC("ciao", 1));
        try {
            long item = input.count();
            System.out.println(item);
        } catch (Exception e) {
            e.printStackTrace();
        }

        tableEnvironment.registerDataSet("words",input);

        Table simpleSelect = tableEnvironment.sql("select * from words");
        Table simpleCount = tableEnvironment.sql("select word, SUM(c) as c from words group by word");
        Table inputTable = tableEnvironment.fromDataSet(input);
        Table directSelect = inputTable
                .groupBy("word")
                .select("word, c.sum as c");


        try {
            List<WC> items = tableEnvironment.toDataSet(simpleSelect, WC.class).collect();
            System.out.println(items);

            items = tableEnvironment.toDataSet(simpleCount, WC.class).collect();
            System.out.println(items);


            items = tableEnvironment.toDataSet(directSelect, WC.class).collect();
            System.out.println(items);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
