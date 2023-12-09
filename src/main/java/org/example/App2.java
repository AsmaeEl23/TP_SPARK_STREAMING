package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.omg.PortableInterceptor.ServerRequestInfo;
import scala.Tuple2;

import java.util.Arrays;

public class App2 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("TP Streaming").setMaster("local[*]");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(8));
        JavaReceiverInputDStream<String> dStream=sc.socketTextStream("localhost",64256);
        JavaDStream<String> dStreamWord=dStream.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        //RDDs
        JavaPairDStream<String,Integer> dPairStream=dStreamWord.mapToPair(m-> new Tuple2<>(m,1));
        JavaPairDStream<String,Integer> dStreamWordsCount=dPairStream.reduceByKey((a,b)->a+b);
        dStreamWordsCount.print();
        sc.start();
        sc.awaitTermination();
    }
}

