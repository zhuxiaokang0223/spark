package com.zxk.spark.streaming.example;

import com.zxk.utils.mysql.ConnectionPool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Describe: 示例需求1
 *
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/7/3 16:29
 * Attention: 数据来源Kafka
 * Modify:
 */
public class LogAnalysisExample1 {

    static final String brokers = System.getProperty("kafka_brokers");
    static final String topics = "spark-test";
    static final String groupId = "gspark-1";


    public static void main(String[] args) throws Exception{
        // Spark Streaming配置
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingOnKafka");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Kafka配置
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建一个InputDStream，这个InputDStream将从Kafka获取数据
        JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> str = lines.map(line -> line.value());

        str.foreachRDD(rdd -> {
            rdd.foreachPartition(eachPartition -> {
                Connection conn = ConnectionPool.getConnection();
                eachPartition.forEachRemaining(record ->{
                    String sql = "insert into log(content,create_time) values('" + record + "', '"+System.currentTimeMillis()+"')";
                    Statement stmt = null;
                    try {
                        stmt = conn.createStatement();
                        stmt.executeUpdate(sql);
                    } catch (java.sql.SQLException e) {
                        e.printStackTrace();
                    }
                    ConnectionPool.returnConnection(conn);
                });
            });
        });

        // 启动JobScheduler, 进而启动 JobGenerator 和 ReceiverTracker
        jsc.start();

        jsc.awaitTermination();
        jsc.close();
    }
}
