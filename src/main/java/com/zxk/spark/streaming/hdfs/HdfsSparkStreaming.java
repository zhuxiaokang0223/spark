package com.zxk.spark.streaming.hdfs;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

/**
 * Describe: Spark streaming 实时分析 HDFS数据
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/6/26 17:23
 * Attention:
 * Modify:
 */
public class HdfsSparkStreaming {

    static final String HDFS_URI = System.getProperty("hdfs_uri");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("SparkStreamingOnHDFS");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaDStream<String> lines  = jsc.textFileStream(HDFS_URI+"/sparktest");
        lines.print();
        //4.2.1 读取数据并对每一行中的数据以空格作为split参数切分成单词以获得DStream<String>
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                System.err.println("====================="+ line);
//                return Arrays.asList(line.split(" ")).iterator();
//            }
//        });

        //4.2.2 使用mapToPair创建PairDStream
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public scala.Tuple2<String, Integer> call(String word) throws Exception {
                return new scala.Tuple2<String, Integer>(word, 1);
            }
        });
        //4.2.3 使用reduceByKey进行累计操作
        JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordsCount.print();
        /*
        * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
         * 接受应用程序本身或者Executor中的消息；
         */
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}