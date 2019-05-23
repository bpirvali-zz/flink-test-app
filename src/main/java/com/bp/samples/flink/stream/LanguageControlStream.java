package com.bp.samples.flink.stream;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class LanguageControlStream {
    public static void main(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "E0yW7HFSW5QygInRJ21cT5Xuv");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "mw6IXsF6AtBur57nkOm36LBawv1cRlgfAARmc0XXAxLNHhuUVK");
        props.setProperty(TwitterSource.TOKEN, "48848806-kuTRxwqheacgHt6WMlxscV0HA3y3RFhDSSoL3uG19");
        props.setProperty(TwitterSource.TOKEN_SECRET, "LAvMloxabdCyKnQ1q01MvrZrxYhzjVmObMtd4cqali11y");

        // lines --> LanguageConfig-objects
        FlatMapFunction linesToLangCfg = new FlatMapFunction<String, LanguageConfig>() {
            @Override
            public void flatMap(String value, Collector<LanguageConfig> out) throws Exception {

                for (String languageConfig : value.split(",")) {
                    String[] kvPair = languageConfig.split("=");
                    out.collect(new LanguageConfig(kvPair[0], Boolean.parseBoolean(kvPair[1])));
                }
            }
        };

        // Tweet keyed by Lang code
        KeySelector tweetKeyedByLangCode = new KeySelector<Tweet, String>() {
            @Override
            public String getKey(Tweet tweet) throws Exception {
                return tweet.getLanguage();
            }
        };

        // LanguageConfig keyed by Lang Code
        KeySelector langCfgKeyedByLangCode = new KeySelector<LanguageConfig, String>() {
            @Override
            public String getKey(LanguageConfig langConfig) throws Exception {
                return langConfig.getLanguage();
            }
        };

        DataStream<LanguageConfig> controlStream = env.socketTextStream("localhost", 9876)
                .flatMap(linesToLangCfg);

        env.addSource(new TwitterSource(props))
                .map(new MapToTweets())
                .keyBy(tweetKeyedByLangCode)
                .connect(controlStream.keyBy(langCfgKeyedByLangCode))
                .flatMap(new RichCoFlatMapFunction<Tweet, LanguageConfig, Tuple2<String, String>>() {
                    ValueStateDescriptor<Boolean> shouldProcess =
                            new ValueStateDescriptor<Boolean>("languageConfig", Boolean.class);

                    @Override
                    public void flatMap1(Tweet tweet, Collector<Tuple2<String, String>> out) throws Exception {
                        Boolean processLanguage = getRuntimeContext().getState(shouldProcess).value();
                        if (processLanguage != null && processLanguage) {
                            for (String tag : tweet.getTags()) {
                                out.collect(new Tuple2<>(tweet.getLanguage(), tag));
                            }
                        }
                    }

                    @Override
                    public void flatMap2(LanguageConfig config, Collector<Tuple2<String, String>> out) throws Exception {
                        getRuntimeContext().getState(shouldProcess).update(config.isShouldProcess());
                    }
                })
                .print();

        env.execute();
    }


    static class LanguageConfig {
        private String language;
        private boolean shouldProcess;

        public LanguageConfig(String language, boolean shouldProcess) {
            this.language = language;
            this.shouldProcess = shouldProcess;
        }

        public String getLanguage() {
            return language;
        }

        public boolean isShouldProcess() {
            return shouldProcess;
        }
    }
}
