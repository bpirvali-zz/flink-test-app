package com.bp.samples.flink.stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

public class NumberOfTweetsPerLanguage {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "E0yW7HFSW5QygInRJ21cT5Xuv");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "mw6IXsF6AtBur57nkOm36LBawv1cRlgfAARmc0XXAxLNHhuUVK");
        props.setProperty(TwitterSource.TOKEN, "48848806-kuTRxwqheacgHt6WMlxscV0HA3y3RFhDSSoL3uG19");
        props.setProperty(TwitterSource.TOKEN_SECRET, "LAvMloxabdCyKnQ1q01MvrZrxYhzjVmObMtd4cqali11y");

        // key to use to split the stream into logical streams
        KeySelector keySelector = new KeySelector<Tweet, String>() {
            @Override
            public String getKey(Tweet tweet) throws Exception {
                return tweet.getLanguage();
            }
        };

        // WindowFunction<IN, OUT, KEY, WINDOW>
        WindowFunction windowFunction = new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {

            @Override         //     KEY                  WINDOW           IN                      OUT
            public void apply(String language, TimeWindow window, Iterable<Tweet> input, Collector<Tuple3<String, Long, Date>> out) throws Exception {
                long count = 0;

                for (Tweet tweet : input) {
                    count++;
                }
                out.collect(new Tuple3<>(language, count, new Date(window.getEnd())));
            }
        };

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        env.addSource(new TwitterSource(props))
                .map(new MapToTweets())
                .keyBy(keySelector)
                .timeWindow(Time.minutes(1))
                .apply(windowFunction)
                .print();
        env.execute();
    }
}
