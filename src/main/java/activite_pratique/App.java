package activite_pratique;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class App {
    public static void main(String[] args) throws Exception {
        SparkSession ss=SparkSession.builder().appName("Structured Streaming").master("local[*]").getOrCreate();

        Dataset<Row> inputTable=ss.readStream()
                .format("socket")
                .option("host","localhost")
                .option("port",64256)
                .load();
        Dataset<String> words=inputTable.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>) line-> Arrays.asList(line.split(" ")).iterator(),Encoders.STRING() );
        //Id, titre, description, service, date
        //1. Afficher d’une manière continue le nombre d’incidents par service.
        Dataset<Row> resultTable1=words.groupBy("service").count();
        //2. Afficher d’une manière continue les deux année ou il a y avait plus d’incidents.
        Dataset<Row> resultTable2=words.groupBy(words.col("date")).count().limit(2);
        StreamingQuery query1=resultTable1.writeStream()
                .format("console")
                .outputMode("complete") //pour utiliser complete if faut utiliser une aggregation dans query
                //append, complete, update
                .start();
        StreamingQuery query2=resultTable2.writeStream()
                .format("console")
                .outputMode("complete") //pour utiliser complete if faut utiliser une aggregation dans query
                //append, complete, update
                .start();

        query1.awaitTermination();
        query2.awaitTermination();


    }
}
