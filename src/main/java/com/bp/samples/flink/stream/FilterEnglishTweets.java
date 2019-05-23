package com.bp.samples.flink.stream;

import com.bp.samples.flink.util.TwitterKeys;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.*;

public class FilterEnglishTweets {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new TwitterSource(TwitterKeys.getTwitterKeys()))
                .map(new MapToTweets())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return tweet.getLanguage().equals("en");
                    }
                })
                .print();

        env.execute();
    }
}

