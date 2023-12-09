package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class App1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("TP Streaming").setMaster("local[*]");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(8));
        JavaReceiverInputDStream<String> dStream=sc.socketTextStream("localhost",64256);
        dStream.print();
        sc.start();
        sc.awaitTermination();
    }
}
