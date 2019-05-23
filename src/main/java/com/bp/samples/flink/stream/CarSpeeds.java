package com.bp.samples.flink.stream;

import com.bp.samples.flink.util.StreamUtil;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class CarSpeeds {
    public static final int SPEED_LIMIT = 65;
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<String> averageViewStream = dataStream
                .map(new Speed())
                .keyBy(0)
//                .flatMap(new AverageSpeedReducingState());
                .flatMap(new AverageSpeedListState());

        averageViewStream.print();

        env.execute("Car Speeds");
    }

    public static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {

        public Tuple2<Integer, Double> map(String row)
                throws Exception {

            return Tuple2.of(1, Double.parseDouble(row));
        }
    }

//    public static class AverageSpeedValueState
//            extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {
//
//        private transient ValueState<Tuple2<Integer, Double>> countSumState;
//        public static final int SPEED_LIMIT = 65;
//        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out)
//                throws Exception {
//
//            Tuple2<Integer, Double> currentCountSum = countSumState.value();
//
//            if (input.f1 >= SPEED_LIMIT) {
//
//                out.collect(String.format(
//                        "SPEED LIMIT EXCEEDED! The average speed of the last %s car(s) was %s,"
//                        + " your speed is %s",
//                        currentCountSum.f0,
//                        currentCountSum.f1 / currentCountSum.f0,
//                        input.f1));
//
//                countSumState.clear();
//                currentCountSum = countSumState.value();
//            } else {
//                out.collect(String.format("THANK-YOU: Your speed:%s, speed limit: %s", input.f1, SPEED_LIMIT) );
//            }
//
//            currentCountSum.f0 += 1;
//
//            currentCountSum.f1 += input.f1;
//
//            countSumState.update(currentCountSum);
//        }
//
//        public void open(Configuration config) throws IOException {
//
//            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor =
//                    new ValueStateDescriptor<Tuple2<Integer, Double>>(
//                            "carAverageSpeed",
//                            TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() { }));
//
//            countSumState = getRuntimeContext().getState(descriptor);
//            countSumState.update(Tuple2.of(0, 0.0));
//        }
//
//    }


    public static class AverageSpeedListState
            extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ListState<Double> speedListState;

        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out)
                throws Exception {

            if (input.f1 >= 65) {

                Iterable<Double> carSpeeds = speedListState.get();
                int count = 0;
                double sum = 0;

                for (Double carSpeed : carSpeeds) {
                    count++;
                    sum += carSpeed;
                }

                out.collect(String.format(
                        "EXCEEDED! The average speed of the last "
                                + "%s car(s) was %s, your speed is %s",
                        count, sum / count, input.f1));

                speedListState.clear();
            } else {
                out.collect("Thank you for staying under the speed limit!");
            }

            speedListState.add(input.f1);
        }

        public void open(Configuration config) {
            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<Double>(
                    "carAverageSpeed", Double.class);
            speedListState = getRuntimeContext().getListState(descriptor);
        }
    }

    public static class AverageSpeedReducingState
            extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Integer> stateNumCars;
        private transient ReducingState<Double> stateSumCarSpeeds;

        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out)
                throws Exception {

            if (input.f1 >= SPEED_LIMIT) {
                double sumSpeed = stateSumCarSpeeds.get();
                int count = stateNumCars.value();

                out.collect(String.format(
                        "SPEED LIMIT (%s) EXCEEDED! #cars:%s, average-speed:%s, your-speed:%s!!!",
                        SPEED_LIMIT, count, sumSpeed / count, input.f1));

                stateNumCars.clear();
                stateSumCarSpeeds.clear();
            } else {
                out.collect(String.format("SPEED LIMIT (%s) FOLLOWED: Your speed:%s",
                        SPEED_LIMIT, input.f1));
            }
            if (stateNumCars.value()==null)
                stateNumCars.update(1);
            else
                stateNumCars.update(stateNumCars.value() + 1);
            stateSumCarSpeeds.add(input.f1);
        }

        public void open(Configuration config) throws IOException {
            ValueStateDescriptor<Integer> valueStateDescriptor =
//                    new ValueStateDescriptor<Integer>("carCount", Integer.class, 0);
                    new ValueStateDescriptor<Integer>("carCount", Integer.class);

            stateNumCars = getRuntimeContext().getState(valueStateDescriptor);
            //stateNumCars.update(Integer.valueOf(0));

            ReducingStateDescriptor<Double> reducingStateDescriptor =
                    new ReducingStateDescriptor<Double>(
                            "SumSpeed",
                            new ReduceFunction<Double>() {
                                public Double reduce(Double cumulative, Double input) {
                                    return cumulative + input;
                                }
                            },
                            Double.class);

            stateSumCarSpeeds = getRuntimeContext().getReducingState(reducingStateDescriptor);
            //stateSumCarSpeeds.clear();
        }
    }

}



















