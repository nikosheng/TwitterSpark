package twitterhotsearch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 基于滑动窗口的热点搜索词实时统计
 *
 * @author Administrator
 */
public class WindowHotWord {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WindowHotWord");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

//        JavaReceiverInputDStream<String> searchLogsDStream = jssc.socketTextStream("student27-x1", 9999);

        Set<String> topics = new HashSet<String>();
        topics.add(args[0].toString());

        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put(args[0].toString(), 1);
        JavaPairReceiverInputDStream<String, String> searchLogsDStream = KafkaUtils.createStream(
                jssc,
                "student27-x1:2181,student27-x2:2181,student28-x1:2181,student28-x2:2181",
                "DefaultConsumerGroup",
                topicThreadMap
        );

        JavaDStream<String> searchWordsDStream = searchLogsDStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2();
            }
        });

        // 将搜索日志给转换成，只有一个搜索词，即可
//        JavaDStream<String> searchWordsDStream = searchLogsDStream.map(new Function<String, String>() {
//
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public String call(String searchLog) throws Exception {
//                return searchLog.split(" ")[1];
//            }
//
//        });

        // 将搜索词映射为(searchWord, 1)的tuple格式
        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordsDStream.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String searchWord)
                            throws Exception {
                        return new Tuple2<String, Integer>(searchWord, 1);
                    }

                });

        // 针对(searchWord, 1)的tuple格式的DStream，执行reduceByKeyAndWindow，滑动窗口操作
        // 第二个参数，是窗口长度，这里是60秒
        // 第三个参数，是滑动间隔，这里是10秒
        // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续
        // 计算
        // 所以说，这里的意思，就是，之前的searchWordPairDStream为止，其实，都是不会立即进行计算的
        // 而是只是放在那里
        // 然后，等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔是，5秒，所以之前
        // 60秒，就有12个RDD，给聚合起来，然后，统一执行redcueByKey操作
        // 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对某个DStream中的RDD
        JavaPairDStream<String, Integer> searchWordCountsDStream =

                searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                }, Durations.seconds(60), Durations.seconds(10));

        // 到这里为止，就已经可以做到，每隔10秒钟，出来，之前60秒的收集到的单词的统计次数
        // 执行transform操作，因为，一个窗口，就是一个60秒钟的数据，会变成一个RDD，然后，对这一个RDD
        // 根据每个搜索词出现的频率进行排序，然后获取排名前3的热点搜索词
        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(

                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaPairRDD<String, Integer> call(
                            JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {
                        // 执行搜索词和出现频率的反转
                        JavaPairRDD<Integer, String> countSearchWordsRDD = searchWordCountsRDD
                                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<Integer, String> call(
                                            Tuple2<String, Integer> tuple)
                                            throws Exception {
                                        return new Tuple2<Integer, String>(tuple._2, tuple._1);
                                    }
                                });

                        // 然后执行降序排序
                        JavaPairRDD<Integer, String> sortedCountSearchWordsRDD = countSearchWordsRDD
                                .sortByKey(false);

                        // 然后再次执行反转，变成(searchWord, count)的这种格式
                        JavaPairRDD<String, Integer> sortedSearchWordCountsRDD = sortedCountSearchWordsRDD
                                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<String, Integer> call(
                                            Tuple2<Integer, String> tuple)
                                            throws Exception {
                                        return new Tuple2<String, Integer>(tuple._2, tuple._1);
                                    }

                                });

                        // 然后用take()，获取排名前3的热点搜索词
//                        List<Tuple2<String, Integer>> hogSearchWordCounts =
//                                sortedSearchWordCountsRDD.take(3);
//                        for (Tuple2<String, Integer> wordCount : hogSearchWordCounts) {
//                            System.out.println(wordCount._1 + ": " + wordCount._2);
//                        }

                        return searchWordCountsRDD;
                    }

                });

        finalDStream.print();
        try {
//            finalDStream.saveAsHadoopFiles("hdfs://student27-x1:9000/wordcount/record", "txt", Text.class, IntWritable.class, TextOutputFormat.class);
            jssc.start();
            jssc.awaitTermination();
            jssc.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jssc.close();
        }

    }

}

