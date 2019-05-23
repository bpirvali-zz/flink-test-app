package com.bp.samples.flink.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;

public class AverageRating {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // movieId,title,genres
        DataSet<Tuple3<Long, String, String>> movies = env.readCsvFile("./data/ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .includeFields(true, true, true)
                .types(Long.class, String.class, String.class);

        // userId,movieId,rating,timestamp
        DataSet<Tuple2<Long, Double>> ratings = env.readCsvFile("./data/ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class);


        JoinFunction joinFunction = new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>,
                Tuple4<Long, String, String, Double>>() {
            @Override
            public Tuple4<Long, String, String, Double> join(Tuple3<Long, String, String> movie, Tuple2<Long, Double> rating) throws Exception {
                long movieId = movie.f0;
                String name = movie.f1;
                String genre = movie.f2.split("\\|")[0];
                Double score = rating.f1;
                return new Tuple4<>(movieId, name, genre, score);
            }
        };

        GroupReduceFunction groupReduceFunction = new GroupReduceFunction<Tuple4<Long, String,String,Double>, Tuple3<String, Integer, Double>>() {
            @Override
            public void reduce(Iterable<Tuple4<Long, String, String, Double>> movies, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {
                String genre = null;
                int count = 0;
                double totalScore = 0;
                for (Tuple4<Long, String, String, Double> movie : movies) {
                    genre = movie.f2;
                    totalScore += movie.f3;
                    count++;
                }
                //System.out.println("----> " + genre + ":" + count);
                collector.collect(new Tuple3<String, Integer, Double>(genre, count, totalScore / count));

            }
        };

//        movies.join(ratings).
//                where(0).
//                equalTo(0).
//                with(joinFunction).
//                //returns(new TypeHint<Tuple4<Long, String, String, Double>>(){}).
//                print();

//        movies.join(ratings)
//                .where(0)
//                .equalTo(0)
//                .with(joinFunction)
//                .groupBy(2)
//                .reduceGroup(groupReduceFunction)
//                .print()
//                //.collect()
                ;

        List<Tuple3<String, Integer, Double>> genreAvgRatings = movies.join(ratings)
                .where(0)
                .equalTo(0)
                .with(joinFunction)
                .groupBy(2)
                .reduceGroup(groupReduceFunction)
                //.print()
                .collect()
        ;

        String res = genreAvgRatings.stream()
                .sorted((r1, r2) -> Double.compare(r1.f2, r2.f2))
                .map(Object::toString)
                .collect(Collectors.joining("\n"));

        System.out.println("==========================================");
        System.out.println(res);
    }
}
