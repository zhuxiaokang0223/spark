package com.zxk.spark.streaming.flume;/**
 * Created by zhuxiaokang on 2018/6/26.
 */

import com.zxk.utils.mysql.ConnectionPool;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import java.sql.Connection;

/**
 * Describe: Spark streaming 实时分析 Flume数据（pull模式）
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/6/26 17:23
 * Attention:
 * Modify:
 */
public class FlumeSparkStream {

    static final String FLUME_SINKS_SPARK_HOSTNAME = System.getProperty("flume_sinks_spark_hostname");

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("SparkStreamingOnFlume");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaReceiverInputDStream<SparkFlumeEvent> flumeEvent = FlumeUtils.createPollingStream(jsc, FLUME_SINKS_SPARK_HOSTNAME, 9999);
//        flumeEvent.print();


        /*JavaDStream<String> w11 = flumeEvent.map(event -> {
            String s = new String(event.event().getBody().array());
            if (s.contains("userLoginForApp")) {
                s = "userLoginForApp";
            }
            return s;
        });*/
//
//        w11.print();

        /*JavaDStream w12 = w11.count();
        w12.print();*/

        JavaDStream<String> w1 = flumeEvent.map(event -> new String(event.event().getBody().array()));
        w1.print();



/*//        JavaDStream<String> w2 = flumeEvent.filter(event -> new String(event.event().getBody().array()).contains("aaaaaa"));
        JavaDStream<String> w2 = w1.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.contains("userLoginForApp");
            }
        });

        w2.print();*/

//        JavaDStream<String> words = flumeEvent.flatMap(event -> Arrays.asList(new String(event.event().getBody().array()).split(",")).iterator());
//        words.print();

        // 保存到本地文件
//        w1.foreachRDD(line -> line.saveAsTextFile("D:\\sparkTestFile\\20180628\\login.txt"));

        // 保存到mysql
        w1.foreachRDD(rdd -> {
            rdd.foreachPartition(eachPartition -> {
                Connection conn = ConnectionPool.getConnection();
                eachPartition.forEachRemaining(record ->{
                    String sql = "insert into log(content) values('" + record + "')";
                    java.sql.Statement stmt = null;
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
        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
