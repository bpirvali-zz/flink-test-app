package com.bp.samples.flink.stream;

import com.bp.samples.flink.util.TwitterKeys;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

public class TopHashTag {
    public static void main(String[] args) throws Exception {
        // 1) get exec-env (local or remote)
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2) set the time characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // tweet --> hash-tags
        FlatMapFunction tweetToHashTags = new FlatMapFunction<Tweet, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tweet tweet, Collector<Tuple2<String, Integer>> collector)  {
                for (String tag : tweet.getTags()) {
                    collector.collect(new Tuple2<>(tag, 1));
                }
            }
        };

        // find top hash-tag in the time window
        AllWindowFunction findTopHashTag =
                new AllWindowFunction<Tuple2<String,Integer>, Tuple3<Date, String, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<Date, String, Integer>> collector)  {

                        String topHashTag = null;
                        int count = 0;
                        for (Tuple2<String, Integer> hashTag : iterable) {
                            if (hashTag.f1 > count) {
                                topHashTag = hashTag.f0;
                                count = hashTag.f1;
                            }
                        }

                        collector.collect(new Tuple3<>(new Date(timeWindow.getEnd()), topHashTag, count));
                    }
                };

        // 3) add source
        // 4) do transforms
        // 5) pass it to sink or print
        env.addSource(new TwitterSource(TwitterKeys.getTwitterKeys()))
                .map(new MapToTweets())
                .flatMap(tweetToHashTags)
                .keyBy(0)
                .timeWindow(Time.minutes(1))
                .sum(1)
                .timeWindowAll(Time.minutes(1))
                .apply(findTopHashTag)
                .print();

        // 6) kick off start
        env.execute();
    }
}
