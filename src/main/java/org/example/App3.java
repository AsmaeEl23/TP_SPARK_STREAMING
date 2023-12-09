package org.example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;


public class App3 {
    public static void main(String[] args) throws Exception {
        SparkSession ss=SparkSession.builder().appName("Structured Streaming").master("local[*]").getOrCreate();

        Dataset<Row> inputTable=ss.readStream()
                .format("socket")
                .option("host","localhost")
                .option("port",64256)
                .load();
        Dataset<String> words=inputTable.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>)line-> Arrays.asList(line.split(" ")).iterator(),Encoders.STRING() );
        Dataset<Row> resultTable=words.groupBy("value").count();
        StreamingQuery query=resultTable.writeStream()
                .format("console")
                .outputMode("complete") //pour utiliser complete if faut utiliser une aggregation dans query
                //append, complete, update
                .start();
        query.awaitTermination();

    }
}
