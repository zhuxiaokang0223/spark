package com.zxk.spark.streaming.kafka;

import com.zxk.utils.mysql.ConnectionPool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.sql.Connection;
import java.util.*;

/**
 * Describe: Spark streaming 实时分析 Kafka数据
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/6/29 13:45
 * Attention: 此示例列出基本函数使用方法
 * Modify:
 */
public class KafkaSparkStreaming {

    static final String brokers = System.getProperty("kafka_brokers");
    static final String topics = "spark-test";
    static final String groupId = "gspark-1";

    private static JavaInputDStream<ConsumerRecord<String, String>> lines = null;
    private static JavaStreamingContext jsc = null;
    private static Set<String> topicsSet = null;
    private static Map<String, Object> kafkaParams = null;

    /**
     * setLogLevel对Spark streaming应用无效。
        解决办法：将spark-core中的log4j-defaults.properties拷贝到resources中并命名为log4j.properties，然后修改日志级别即可
        org.apache.spark.api.java.JavaSparkContext sc = new org.apache.spark.api.java.JavaSparkContext(conf);
        sc.setLogLevel(org.apache.log4j.Level.WARN.toString());
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(10));
     */
    public KafkaSparkStreaming() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingOnKafka");
        jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaRecever();
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaSparkStreaming kss = new KafkaSparkStreaming();
        JavaDStream<String> ds = kss.map();

        // 执行示例
//        flatMap(ds);
//        flatMapToPair(ds);
//        saveToMysql(ds);
//        filter(ds);
//        union(ds);
//        count(ds);
//        reduce(ds);
//        countByValue(ds);
//        mapToPair(ds);
//        reduceByKey(ds);
        window(ds);

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

    /**
     * Kafka数据接收器
     */
    public void kafkaRecever() {
        lines = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
    }

    /********************************* Transformations *********************************/

    /**
     * Transformations： map(func)
     *
     * 返回一个新的DStream，通过一个函数func传递源DStream的每个元素。
     * @return
     */
    public JavaDStream map(){
        // 数据源示例： 9,ABCD-APP,86992702677417813123,OPPO R9t,4g,20180628080003,methodInvoke
        // 数据结构示例： 9,ABCD-APP,86992702677417813123,OPPO R9t,4g,20180628080003,methodInvoke
        JavaDStream<String> ds = lines.map(line -> line.value());
        ds.print();
        return ds;
    }

    /**
     * Transformations： mapToPari(func)
     *
     * @return
     */
    public static void mapToPair(JavaDStream<String> ds){
        // 数据源示例：
        /*
            (ABCD-APP,869927026774178,1)
            (ABCD-BBB,131313313131231,1)
         */
        // 数据结构示例：
        /*
           ABCD-APP,869927026774178
           ABCD-BBB,131313313131231,1
         */
        JavaPairDStream<String, Integer> rdd = ds.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public scala.Tuple2<String, Integer> call(String s) throws Exception {
                return new scala.Tuple2<String, Integer>(s, 1);
            }
        });
        rdd.print();
    }

    /**
     * Transformations： flatMap(func)
     *
     * 类似于map，但是每个输入项都可以映射到0或更多的输出项。
     * @param ds
     */
    public static void flatMap(JavaDStream<String> ds){
        // 将数据以“,”分割：
        // 数据源示例： ABCD-APP,869927026774178
        /* 数据结构示例：
          ABCD-APP
          869927026774178
        */
        JavaDStream<String> rdd =  ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        rdd.print();
        rdd.count().print();
    }

    /**
     * Transformations： flatMapToPair(func)
     *
     * mapToPair是一对一，一个元素返回一个元素，而flatMapToPair可以一个元素返回多个，相当于先flatMap,在mapToPair
     * @param ds
     */
    public static void flatMapToPair(JavaDStream<String> ds) {
        // 将数据以“,”分割
        // 数据源示例： ABCD-APP,869927026774178
        /* 数据结构示例：
         (ABCD-APP,1)
         (869927026774178,1)
        */
        JavaPairDStream<String, Object> rdd = ds.flatMapToPair(new PairFlatMapFunction<String, String, Object>() {
            @Override
            public Iterator<scala.Tuple2<String, Object>> call(String s) throws Exception {
                List<scala.Tuple2<String, Object>> results = new ArrayList<>();
                String[] str = s.split(",");
                for (String s1 : str) {
                    results.add(new scala.Tuple2<String, Object>(s1, 1));
                }
                return results.iterator();
            }
        });
        rdd.print();
        rdd.count().print();
    }

    /**
     * Transformations： filter(func)
     *
     * 通过只选择func返回true的源DStream的记录返回一个新的DStream。
     * @param ds
     */
    public static void filter(JavaDStream<String> ds) {
        // 数据源示例：
        /*
            ABCD-APP,869927026774178
            ABCD-BBB,869927026774178
         */
        // 数据结构示例：
        /*
            ABCD-APP,869927026774178
         */

        // 过滤每一个元素，应用func方法进行计算，如果func函数返回结果为true，则保留该元素，否则丢弃该元素，返回一个新的DStream
        JavaDStream<String> rdd = ds.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                boolean result = !s.contains("ABCD-BBB");
                return result;
            }
        });
        rdd.print();
    }

    /**
     * 通过创建更多或更少的分区来改变这个DStream中的并行度。
     */
    public static void repartition() {

    }

    /**
     * Transformations： union(otherStream)
     *
     * 返回一个新的DStream，它包含源DStream和其他流中的元素的结合。
     * @param ds
     */
    public static void union(JavaDStream<String> ds) {
        // 数据源示例：
        /*
            ABCD-APP,869927026774178
         */
        // 数据结构示例：
        /*
            ABCD-APP,869927026774178,1530676710000
            ABCD-APP,869927026774178,hhhhhhhh
         */

        String time = String.valueOf(System.currentTimeMillis());
        JavaDStream<String> rdd = ds.map(line -> line+","+time);
        JavaDStream<String> rdd1 = ds.map(line -> line+",hhhhhhhh");
        JavaDStream<String> rdd2 = rdd.union(rdd1);
        rdd2.print();
        // union之后执行以下方法会抛异常：KafkaConsumer is not safe for multi-threaded access
//        rdd2.count().print();
    }

    /**
     * Transformations： count()
     *
     * 通过计算源DStream每个RDD中的元素数量，返回一个新的单元素RDDs DStream。
     * @param ds
     */
    public static void count(JavaDStream<String> ds) {
        // 数据源示例：
        /*
            ABCD-APP,869927026774178
         */
        // 数据结构示例：
        /*
            1
         */
       // rdd中元素的数量
       JavaDStream<Long> rdd = ds.count();
       rdd.print();
    }

    /**
     * Transformations： reduce(func)
     *
     * 通过聚合源DStream的每个RDD中的元素来返回一个新的单元素RDDs的DStream。
     * @param ds
     */
    public static void reduce(JavaDStream<String> ds) {
        // 数据源示例：
        /*
            1
            2
            3
            4
         */
        // 数据结构示例：
        /*
            10
         */

        // 本示例使用reduce处理integer类型总数。 当然reduce也可以处理其它类型的数据。这里先将rdd中的元素转为integer类型再使用reduce进行计算。当然也可以直接使用reduce计算。而且类型
        JavaDStream<Integer> rdd = ds.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                String str = s.replace("\r\n","");
                return Integer.parseInt(str);
            }
        });
       // reduce并行计算，返回一个值。
       // reduce将RDD中元素前两个传给输入函数，产生一个新的return值，新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，再被传给输入函数，直到最后只有一个值为止。
       JavaDStream<Integer> rdd1 = rdd.reduce(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer integer, Integer integer2) throws Exception {
               return integer+integer2;
           }
       });
        rdd1.print();
        rdd1.count().print();
    }


    /**
     * Transformations： countByValue()
     *
     * 某个DStream中的元素类型为K，调用这个方法后，返回的DStream的元素为(K, Long)对，后面这个Long值是原DStream中每个RDD元素key出现的频率。
     * @param ds
     */
    public static void countByValue(JavaDStream<String> ds) {
        // 数据源示例：
        /*
            ABCD-ccc,131313313131231
            ABCD-bbb,131313313131231
            ABCD-bbb,131313313131231
         */
        // 数据结构示例：
        /*
            (ABCD-ccc,131313313131231,1)
            (ABCD-bbb,131313313131231,2)
         */
        ds.countByValue().print();
    }

    /**
     * Transformations： reduceByKey(func, [numTasks])
     *
     * 调用这个操作的DStream是以(K, V)的形式出现，返回一个新的元素格式为(K, V)的DStream。返回结果中，K为原来的K，V是由K经过传入func计算得到的。还可以传入一个并行计算的参数，在local模式下，默认为2。在其他模式下，默认值由参数spark.default.parallelism确定。
     * @param ds
     */
    public static void reduceByKey(JavaDStream<String> ds) {
        // 数据源示例：
        /*
            ABCD-ccc,131313313131231
            ABCD-bbb,131313313131231
            ABCD-bbb,131313313131231
         */
        // 数据结构示例：
        /*
            (ABCD-ccc,131313313131231,1)
            (ABCD-bbb,131313313131231,1)
            (ABCD-bbb,131313313131231,1)
         */
        // RDD1数据结构示例：
        /*
            (ABCD-ccc,131313313131231,1)
            (ABCD-bbb,131313313131231,2)
         */
        JavaPairDStream<String, Integer> rdd = ds.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public scala.Tuple2<String, Integer> call(String s) throws Exception {
                return new scala.Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> rdd1 = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        rdd1.print();
    }


    /*********************************** Window ***********************************/

    /**
     * Window： window(windowLength, slideInterval)
     *
     * 该操作由一个DStream对象调用，传入一个窗口长度参数，一个窗口移动速率参数，然后将当前时刻当前长度窗口中的元素取出形成一个新的DStream。
     * @param ds
     */
    public static void window(JavaDStream<String> ds){
       /*
        （1）窗口长度（window length），即窗口的持续时间，上图中的窗口长度为3
        （2）滑动间隔（sliding interval），窗口操作执行的时间间隔，上图中的滑动间隔为2
         这两个参数必须是原始DStream 批处理间隔（batch interval）的整数倍
       */
       JavaDStream<String> rdd = ds.window(Durations.seconds(20), Durations.seconds(10));
       rdd.print();
    }

    /*********************************** Output ***********************************/

    /**
     * Output：foreachRDD(func)
     * 数据保存到mysql
     * @param ds
     */
    public static void saveToMysql(JavaDStream<String> ds) {
        ds.foreachRDD(rdd -> {
            rdd.foreachPartition(eachPartition -> {
                Connection conn = ConnectionPool.getConnection();
                eachPartition.forEachRemaining(record ->{
                    String sql = "insert into log(content,create_time) values('" + record + "', '"+System.currentTimeMillis()+"')";
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
    }
}
