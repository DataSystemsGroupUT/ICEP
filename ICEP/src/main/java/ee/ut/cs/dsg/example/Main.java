/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.example;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.processor.D2IAHomogeneousIntervalProcessorFunction;
import ee.ut.cs.dsg.example.event.*;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.Operator;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.generator.HomogeneousIntervalGenerator;
import ee.ut.cs.dsg.example.mapper.ThroughputRecorder;
import ee.ut.cs.dsg.example.source.FixedSource;
import ee.ut.cs.dsg.example.source.TemperatureSource;
import ee.ut.cs.dsg.d2ia.trigger.GlobalWindowEventTimeTrigger;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.util.*;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;

//import org.uniTartu.cep.interval2.events.TemperatureEvent;
//import org.uniTartu.cep.interval2.events.TemperatureWarning;
//import org.uniTartu.cep.interval2.sources.CEPIntervalSource;


import javax.annotation.Nullable;

//import static java.util.regex.Pattern.union;


/**
 * @author MKamel
 */
public class Main {

    private static final double TEMPERATURE_THRESHOLD = 40;
    private static final int MAX_RACK_ID = 10;
    private static final long PAUSE = 100;
    private static final double TEMP_STD = 20;
    private static final double TEMP_MEAN = 80;
    private static final double power_STD = 10;
    private static final double power_MEAN = 100;

    public static void main(String[] args) throws Exception {

//        testHomogeneousIntervals();

    //    testGlobalWindowWithFixedDataSet();

      //  testMatchRecognize();
         testSQLWithFixedDataSet();
    }

    private static void testHeterogenousIntervals() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);


        List<TemperatureEvent> myTemps = new ArrayList<TemperatureEvent>();

        myTemps.add(new TemperatureEvent("1", 1, 30));
        myTemps.add(new TemperatureEvent("1", 2, 35));
        myTemps.add(new TemperatureEvent("1", 3, 33));
        myTemps.add(new TemperatureEvent("1", 4, 40));
        DataStream<TemperatureEvent> inputEventStream = env
                .fromCollection(myTemps);
//                .addSource(new TemperatureSource(
//                        MAX_RACK_ID,
//                        PAUSE,
//                        TEMP_STD,
//                        TEMP_MEAN))
//               .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        //  DataStream<String> lineardata = env.readTextFile("F:\\TPStream\\linear_accel.events\\linear_accel.events");


//        StreamTableSource<TemperatureEvent> extends TableSource<TemperatureWarning> {
//
//            public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
//        }


//        DataStream<PowerEvent> inputEventStream2 = env
//                .addSource(new PowerSource(
//                        MAX_RACK_ID,
//                        PAUSE,
//                        power_STD,
//                        power_MEAN))
//                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());


        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> newInterval = new HomogeneousIntervalGenerator<>();
//       HomogeneousIntervalGenerator<PowerEvent, TemperatureWarning> newInterval2 = new HomogeneousIntervalGenerator<>();


        newInterval.source(inputEventStream)
                .sourceType(TemperatureEvent.class)
                //.condition(new AbsoluteCondition().operator(Operator.GreaterThan).RHS(30))
                .condition(new RelativeCondition().relativeLHS(Operand.Value).relativeOperator(Operator.GreaterThan).relativeRHS(Operand.Last).operator(Operator.GreaterThanEqual).RHS(30))
                //  .minOccurrences(2)
                .targetType(TemperatureWarning.class)
                //       .maxOccurrences(3)
                .within(Time.milliseconds(5))
                .outputValue(Operand.Max)
                .produceOnlyMaximalIntervals(true);

        DataStream<TemperatureWarning> warning1 = newInterval.runWithCEP();

//        newInterval.source(inputEventStream)
//               .sourceType(TemperatureEvent.class)
//               .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(10))
//               .targetType(TemperatureWarning.class)
//               .outvalue(HomogeneousIntervalGenerator.Operator.Average)
//                .conditionType("R");
//               DataStream<TemperatureWarning> warning1= newInterval.runWithCEP();
//        DataStream<TemperatureWarning> warning2   = newInterval.run_loop_generator(6);

//
//        newInterval.source(lineardata)
//                .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
//                .targetType(TemperatureWarning.class);
//        DataStream<TemperatureWarning> warning11= newInterval.runWithCEP();
//        DataStream<TemperatureWarning> warning22   = newInterval.run_loop_generator(4);
//


        inputEventStream.print();

        warning1.print();
//        warning2.print();


//        newInterval2.source(inputEventStream2)
//                .sourceType(PowerEvent.class)
//                .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
//                .targetType(TemperatureWarning.class)
//                .outvalue(HomogeneousIntervalGenerator.Operator.Last)
//                .conditionType("R");
//              //  .outvalue(HomogeneousIntervalGenerator.Operator.Average);
//                DataStream<TemperatureWarning> power1 = newInterval2.runWithCEP();
//        DataStream<TemperatureWarning> power2 = newInterval2.run_loop_generator(4);
////        power1.print();
////        power2.print();


//        IntervalOperator.before(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.meets(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.equalTo(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.overlap(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.during(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.starts(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.finishes(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.contains(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.startsBy(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.overlapby(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.metBy(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.after(warning1, power1, Time.minutes(1)).print();


        //Step No 5
        //Trigger the programme execution by calling execute(), mode of execution (local or cluster).
        env.execute("CEP Interval job");
    }

    private static void testHomogeneousIntervals() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost",
//                6123, "C:\\Work\\Big Data Lab\\ICEP2\\ICEP\\target\\D2IA-0.1-SNAPSHOT.jar");

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200);
        SingleOutputStreamOperator<Tuple2<Long, String>> counts = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {
                        String[] vals = s.split(" ");
                        return new Tuple2<>(Long.parseLong(vals[0]), vals[1]);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<Long, String>>() {

                    final long maxLateness = 3500;
                    long maxTimestampSeen = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTimestampSeen - maxLateness);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<Long, String> tuple, long l) {
                        maxTimestampSeen = Math.max(maxTimestampSeen, tuple.f0);
                        return tuple.f0;
                    }
                })
                .keyBy(1)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .allowedLateness(Time.milliseconds(100))
                .sum(1);
        counts.print();
 //       env.setParallelism(1);
//        env.readFile("F:\TPStream\linear_accel.events\linear_accel.events","F:\TPStream\linear_accel.events\linear_accel.events","F:\TPStream\linear_accel.events\linear_accel.events");

//        DataSet<Tuple2<String, String>> rawdata =
//                env.readCsvFile("E:\\CrimeReport.csv").includeFields("0000011").ignoreFirstLine()

        List<TemperatureEvent> myTemps = new ArrayList<TemperatureEvent>();

        myTemps.add(new TemperatureEvent("1", 1, 31));
        myTemps.add(new TemperatureEvent("1", 2, 30));
        myTemps.add(new TemperatureEvent("1", 3, 33));
        myTemps.add(new TemperatureEvent("1", 4, 40));
        myTemps.add(new TemperatureEvent("1", 5, 30));
        DataStream<TemperatureEvent> inputEventStream = env
                .fromCollection(myTemps);

//Random Stream
        DataStream<TemperatureEvent> randomStream = env.addSource(new TemperatureSource(1,1,35));
        randomStream = randomStream.map(new ThroughputRecorder());

        List<PowerEvent> myPowers = new ArrayList<>();
        myPowers.add(new PowerEvent("1", 1, 30));
        myPowers.add(new PowerEvent("1", 2, 35));
        myPowers.add(new PowerEvent("1", 3, 31));
        myPowers.add(new PowerEvent("1", 4, 40));
        DataStream<PowerEvent> inputEventStream2 = env
                .fromCollection(myPowers);
//                .addSource(new TemperatureSource(
//                        MAX_RACK_ID,
//                        PAUSE,
//                        TEMP_STD,
//                        TEMP_MEAN))
//               .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        //  DataStream<String> lineardata = env.readTextFile("F:\\TPStream\\linear_accel.events\\linear_accel.events");


//        StreamTableSource<TemperatureEvent> extends TableSource<TemperatureWarning> {
//
//            public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
//        }


//        DataStream<PowerEvent> inputEventStream2 = env
//                .addSource(new PowerSource(
//                        MAX_RACK_ID,
//                        PAUSE,
//                        power_STD,
//                        power_MEAN))
//                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());


//        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> newInterval = new HomogeneousIntervalGenerator<>();
//
//        HomogeneousIntervalGenerator<PowerEvent, PowerWarning> newInterval2 = new HomogeneousIntervalGenerator<>();


//        newInterval.source(randomStream)
//                .sourceType(TemperatureEvent.class)
//                //.condition(new AbsoluteCondition().operator(Operator.GreaterThan).RHS(30))
//                .condition(new RelativeCondition().relativeLHS(Operand.Value).relativeOperator(Operator.GreaterThan).relativeRHS(Operand.Last).operator(Operator.GreaterThanEqual).RHS(30))
//                .minOccurrences(2)
//                .targetType(TemperatureWarning.class)
//                .maxOccurrences(5)
//                .within(Time.milliseconds(10))
//                .outputValue(Operand.Max)
//                .produceOnlyMaximalIntervals(true);
//
//        DataStream<TemperatureWarning> warning1 = newInterval.runWithCEP();


//        Threshold
        HomogeneousIntervalGenerator<TemperatureEvent, ThresholdInterval> threshold = new HomogeneousIntervalGenerator<>();
        threshold.source(randomStream)
                .sourceType(TemperatureEvent.class)
                .targetType(ThresholdInterval.class)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.GreaterThanEqual).RHS(31))
                .within(Time.milliseconds((10)))
 //               .minOccurrences(2)
                .outputValue(Operand.Average)
                .produceOnlyMaximalIntervals(true);
        DataStream<ThresholdInterval> thresholdWarning = threshold.runWithCEP();
//        System.out.println("Threshold");
        thresholdWarning.print();
//
////

        AbsoluteCondition absoluteCondition = new AbsoluteCondition();
        absoluteCondition.operator(Operator.Absolute).RHS(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.Minus).RHS(Operand.Min));
        //Delta
        HomogeneousIntervalGenerator<TemperatureEvent, DeltaInterval> delta = new HomogeneousIntervalGenerator<>();
        delta.source(randomStream)
                .sourceType(TemperatureEvent.class)
                .targetType(DeltaInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(absoluteCondition).relativeOperator(Operator.GreaterThanEqual).relativeRHS(0.01))
                .minOccurrences(2)
//                .maxOccurrences(2)
                .within(Time.milliseconds((100)))
                .outputValue(Operand.Max)
                .produceOnlyMaximalIntervals(true);

        DataStream<DeltaInterval> deltaWarning = delta.runWithCEP();
//        System.out.println("Delta");
        deltaWarning.print();


        HomogeneousIntervalGenerator<TemperatureEvent, AggregateInterval> aggregate = new HomogeneousIntervalGenerator<>();

        aggregate.source(randomStream)
                .sourceType(TemperatureEvent.class)
                .targetType(AggregateInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThan).relativeRHS(36))
                .minOccurrences(2)
//                .maxOccurrences(2)
                .within(Time.milliseconds((100)))
                .outputValue(Operand.Average)
                .produceOnlyMaximalIntervals(true);

        DataStream<AggregateInterval> aggregateWarning = aggregate.runWithCEP();
//        System.out.println("Aggregate");
        aggregateWarning.print();
//        newInterval2.source(inputEventStream2)
//                .sourceType(PowerEvent.class)
//                .condition(new RelativeCondition().relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThan).relativeRHS(30).operator(Operator.GreaterThanEqual).RHS(Operand.Value))
//                .targetType(PowerWarning.class)
//                .within(Time.milliseconds(100))
//                .outputValue(Operand.Average)
//                .produceOnlyMaximalIntervals(true);
//
//        //               DataStream<TemperatureWarning> warning1= newInterval.runWithCEP();
//
//        DataStream<PowerWarning> warning2 = newInterval2.runWithCEP();

//


//        inputEventStream2.print();
//        randomStream.print();
//        warning1.print();
//        warning2.print();

//        IntervalOperator<TemperatureWarning,PowerWarning> matchOperator = new IntervalOperator<>();
//
//        matchOperator.leftIntervalStream(warning1)
//                .rightIntervalStream(warning2)
//                .within(Time.milliseconds(100))
//                .filterForMatchType(Match.MatchType.Equals)
//                .filterForMatchType(Match.MatchType.During)
//                .filterForMatchType(Match.MatchType.Contains)
//                .filterForMatchType(Match.MatchType.Overlaps)
//                .filterForMatchType(Match.MatchType.Starts)
//                .filterForMatchType(Match.MatchType.StartedBy)
//                .filterForMatchType(Match.MatchType.Finishes)
//                .filterForMatchType(Match.MatchType.FinishedBy);
//
//        DataStream<Match> matches = matchOperator.runWithCEP();
//
//        matches.print();

//        newInterval2.source(inputEventStream2)
//                .sourceType(PowerEvent.class)
//                .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
//                .targetType(TemperatureWarning.class)
//                .outvalue(HomogeneousIntervalGenerator.Operator.Last)
//                .conditionType("R");
//              //  .outvalue(HomogeneousIntervalGenerator.Operator.Average);
//                DataStream<TemperatureWarning> power1 = newInterval2.runWithCEP();
//        DataStream<TemperatureWarning> power2 = newInterval2.run_loop_generator(4);
////        power1.print();
////        power2.print();


//        IntervalOperator.before(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.meets(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.equalTo(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.overlap(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.during(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.starts(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.finishes(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.contains(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.startsBy(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.overlapby(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.metBy(warning1, power1, Time.minutes(1)).print();
//        IntervalOperator.after(warning1, power1, Time.minutes(1)).print();


        //Step No 5
        //Trigger the programme execution by calling execute(), mode of execution (local or cluster).
         JobExecutionResult result =  env.execute("CEP Interval job");
         //result.getAccumulatorResult("throughput");
    }

    private static void testGlobalWindowWithFixedDataSet() throws Exception
    {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10);

//        List<TemperatureEvent> myTemps = new ArrayList<TemperatureEvent>();
//
//        myTemps.add(new TemperatureEvent("1", 1, 20));
//        myTemps.add(new TemperatureEvent("1", 2, 20));
//        myTemps.add(new TemperatureEvent("1", 3, 20));
//        myTemps.add(new TemperatureEvent("1", 4, 21));
//        myTemps.add(new TemperatureEvent("W", 4, 21));
//        myTemps.add(new TemperatureEvent("1", 5, 20));
//        myTemps.add(new TemperatureEvent("1", 6, 20));
//        myTemps.add(new TemperatureEvent("1", 7, 20));
//        myTemps.add(new TemperatureEvent("1", 8, 20));
//        myTemps.add(new TemperatureEvent("1", 9, 20));
//        myTemps.add(new TemperatureEvent("W", 9, 21));
//        myTemps.add(new TemperatureEvent("1", 10, 20));
//        myTemps.add(new TemperatureEvent("1", 11, 20));
//        myTemps.add(new TemperatureEvent("1", 12, 20));
//        myTemps.add(new TemperatureEvent("1", 13, 20));
//        myTemps.add(new TemperatureEvent("W", 14, 21));


        DataStream<TemperatureEvent> inputEventStream = env.addSource(new FixedSource());
//        DataStream<TemperatureEvent> inputEventStream = env.fromCollection(myTemps).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TemperatureEvent>() {
//            private long maxTimestampSeen = 0;
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                return new Watermark(maxTimestampSeen);
//            }
//
//            @Override
//            public long extractTimestamp(TemperatureEvent temperatureEvent, long l) {
//                long ts = temperatureEvent.getTimestamp();
//                if (temperatureEvent.getKey().equals("W"))
//                    maxTimestampSeen = Long.max(maxTimestampSeen,ts);
//                return ts;
//            }
//        });

        KeyedStream<TemperatureEvent, String> keyedTemperatureStream = inputEventStream.keyBy(new KeySelector<TemperatureEvent, String>() {
            @Override
            public String getKey(TemperatureEvent temperatureEvent) throws Exception {
                return temperatureEvent.getKey();
            }
        });

        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> testGenerator = new HomogeneousIntervalGenerator<>();
        testGenerator.source(keyedTemperatureStream)
                .sourceType(TemperatureEvent.class)
                .targetType(TemperatureWarning.class)
                .minOccurrences(2)
                .maxOccurrences(-1)
                .outputValue(Operand.Last)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.LessThanEqual).RHS(20))
          //      .produceOnlyMaximalIntervals(true)
                .within(Time.milliseconds(5));

        DataStream<TemperatureWarning> warningsIntervalStream = testGenerator.runWithGlobalWindow();
        warningsIntervalStream.print();
        env.execute("Interval generator via global windows");
    }

    private static void testSQLWithFixedDataSet() throws Exception
    {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10);

//        List<TemperatureEvent> myTemps = new ArrayList<TemperatureEvent>();
//
//        myTemps.add(new TemperatureEvent("1", 1, 20));
//        myTemps.add(new TemperatureEvent("1", 2, 20));
//        myTemps.add(new TemperatureEvent("1", 3, 20));
//        myTemps.add(new TemperatureEvent("1", 4, 21));
//        myTemps.add(new TemperatureEvent("W", 4, 21));
//        myTemps.add(new TemperatureEvent("1", 5, 20));
//        myTemps.add(new TemperatureEvent("1", 6, 20));
//        myTemps.add(new TemperatureEvent("1", 7, 20));
//        myTemps.add(new TemperatureEvent("1", 8, 20));
//        myTemps.add(new TemperatureEvent("1", 9, 20));
//        myTemps.add(new TemperatureEvent("W", 9, 21));
//        myTemps.add(new TemperatureEvent("1", 10, 20));
//        myTemps.add(new TemperatureEvent("1", 11, 20));
//        myTemps.add(new TemperatureEvent("1", 12, 20));
//        myTemps.add(new TemperatureEvent("1", 13, 20));
//        myTemps.add(new TemperatureEvent("W", 14, 21));


        DataStream<TemperatureEvent> inputEventStream = env.addSource(new FixedSource());
//        DataStream<TemperatureEvent> inputEventStream = env.fromCollection(myTemps).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TemperatureEvent>() {
//            private long maxTimestampSeen = 0;
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                return new Watermark(maxTimestampSeen);
//            }
//
//            @Override
//            public long extractTimestamp(TemperatureEvent temperatureEvent, long l) {
//                long ts = temperatureEvent.getTimestamp();
//                if (temperatureEvent.getKey().equals("W"))
//                    maxTimestampSeen = Long.max(maxTimestampSeen,ts);
//                return ts;
//            }
//        });

        KeyedStream<TemperatureEvent, String> keyedTemperatureStream = inputEventStream.keyBy(new KeySelector<TemperatureEvent, String>() {
            @Override
            public String getKey(TemperatureEvent temperatureEvent) throws Exception {
                return temperatureEvent.getKey();
            }
        });

        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> testGenerator = new HomogeneousIntervalGenerator<>();
        testGenerator.source(keyedTemperatureStream)
                .sourceType(TemperatureEvent.class)
                .targetType(TemperatureWarning.class)
                .minOccurrences(2)
     //           .maxOccurrences(-1)
                .outputValue(Operand.Last)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.LessThanEqual).RHS(20))
                //      .produceOnlyMaximalIntervals(true)
                .within(Time.milliseconds(5));

        DataStream<TemperatureWarning> warningsIntervalStream = testGenerator.runWithSQL(env);
        warningsIntervalStream.print();
        env.execute("Interval generator via flink sql match recognize");
    }

    private static void testGlobalWindow() throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStream<TemperatureEvent> temperatureEventDataStream = env.addSource(new FixedSource());
        DataStream<TemperatureEvent> temperatureEventDataStream = env.addSource(new TemperatureSource(1,2,18)).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TemperatureEvent>() {
            private long maxTimestampSeen = 0;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTimestampSeen);
            }

            @Override
            public long extractTimestamp(TemperatureEvent temperatureEvent, long l) {
                long ts = temperatureEvent.getTimestamp();
                maxTimestampSeen = Long.max(maxTimestampSeen,ts);
                return ts;
            }
        });
        KeyedStream<TemperatureEvent, String> keyedTemperatureStream = temperatureEventDataStream.keyBy(new KeySelector<TemperatureEvent, String>() {
            @Override
            public String getKey(TemperatureEvent temperatureEvent) throws Exception {
                return temperatureEvent.getKey();
            }
        });

     //   keyedTemperatureStream.window(GlobalWindows.create()).trigger(new GlobalWindowEventTimeTrigger())
//                .evictor(new WatermarkEvictor())

                /*
                //Commented on 7th August 2019 to replace with a more generic process function
                .process(new ProcessWindowFunction<TemperatureEvent, Tuple4<String, Long, Long, Double>, String, GlobalWindow>() {
            @Override
            public void process(String o, Context context, Iterable<TemperatureEvent> iterable, Collector<Tuple4<String, Long, Long, Double>> collector) throws Exception {
                ArrayList<TemperatureEvent> sorted = new ArrayList();

                for ( TemperatureEvent e: iterable) {
                    //Enforce watermark rule
//                    if (e.getTimestamp() <= context.currentWatermark())
                        sorted.add(e);

                }
                Collections.sort(sorted, new Comparator<TemperatureEvent>(){

                    @Override
                    public int compare(TemperatureEvent o1, TemperatureEvent o2) {
                        if (o1.getTimestamp() < o2.getTimestamp()) return -1;
                        if (o1.getTimestamp() > o2.getTimestamp()) return 1;
                        return 0;
                    }
                });

                //let's say we output threshold intervals with condition temperature <= 21
                long start=0, end=0;
                double value=0;
                int i =0;
                TemperatureEvent event=null;
                boolean brokenFromLoop=false;
                for (; i < sorted.size();i++)
                {

                    event = sorted.get(i);
//                    if (event.getTimestamp() > context.currentWatermark() && event.getValue() <= 20.0)
//                        return; // no need to process next elements and shouldn't emit an incomplete window
                    if (event.getTimestamp() > context.currentWatermark()) {
                        brokenFromLoop=true;
                        break; // no need to process the rest of the elements but we can emit the current complete window, if any
                    }
                    if (event.getValue() <= 20.0)
                    {
                        if (start==0) // we start a new interval
                            start = event.getTimestamp();
                        end = event.getTimestamp();
                        value+= event.getValue();
                    }
                    else
                    {
                        if (start !=0) {
                            collector.collect(new Tuple4<>(o, start, end, value));
                            start = 0;
                            end = 0;
                            value = 0;
                        }
                    }
                }
                long keepuntilTs=context.currentWatermark();
                if (start !=0 && !brokenFromLoop) // we processed all elements normally
                    collector.collect(new Tuple4<>(o,start,end,value));
                else if (start != 0 && brokenFromLoop && event.getValue() > 20.0) // we received an item with future timestamp (greater than watermark) but its value is breaking the theta condition
                    collector.collect(new Tuple4<>(o,start,end,value));
                else if (start!=0 && brokenFromLoop && event.getValue() <= 20.0)
                    keepuntilTs = start;
                // ugly but necessary, to clean here not in the evictor, actually, I will drop the evictor
                for (Iterator<TemperatureEvent> iterator = iterable.iterator(); iterator.hasNext();){
                    TemperatureEvent v =  iterator.next();
                    if (v.getTimestamp() < keepuntilTs)
                    {
                        iterator.remove();
                    }

                }
            }

        })*/
//        .process(new D2IAHomogeneousIntervalProcessorFunction<TemperatureEvent,ThresholdInterval>(0,0, null, null, Operand.Max), TypeInformation.of(ThresholdInterval.class))
//        .print();

        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> testGenerator = new HomogeneousIntervalGenerator<>();
        testGenerator.source(keyedTemperatureStream)
                .sourceType(TemperatureEvent.class)
                .targetType(TemperatureWarning.class)
                .minOccurrences(5)
             //   .maxOccurrences(5)
                .outputValue(Operand.Last)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.LessThanEqual).RHS(20))
                .produceOnlyMaximalIntervals(true)
                .within(Time.milliseconds(10));

        DataStream<TemperatureWarning> warningsIntervalStream = testGenerator.runWithGlobalWindow();
        warningsIntervalStream.print();
        env.execute("Interval generator via global windows");

    }

    private static void testMatchRecognize() throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

//        DataStream<TemperatureEvent> temperatureEventDataStream = env.addSource(new FixedSource());
        DataStream<TemperatureEvent> temperatureEventDataStream = env.addSource(new TemperatureSource(1,2,18)).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TemperatureEvent>() {
            private long maxTimestampSeen = 0;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTimestampSeen);
            }

            @Override
            public long extractTimestamp(TemperatureEvent temperatureEvent, long l) {
                long ts = temperatureEvent.getTimestamp();
                maxTimestampSeen = Long.max(maxTimestampSeen,ts);
                return ts;
            }
        });
        KeyedStream<TemperatureEvent, String> keyedTemperatureStream = temperatureEventDataStream.keyBy((KeySelector<TemperatureEvent, String>) temperatureEvent -> temperatureEvent.getKey());

        TupleTypeInfo<Tuple3<String, Double, Long>> inputTupleInfo = new TupleTypeInfo<>(
                Types.STRING(),
                Types.DOUBLE(),
                Types.LONG()
        );
        tableEnv.registerDataStream("RawEvents",
                keyedTemperatureStream.map((MapFunction<TemperatureEvent, Tuple3<String, Double, Long>>) temperatureEvent -> new Tuple3<>(temperatureEvent.getKey(),temperatureEvent.getValue(),temperatureEvent.getTimestamp())).returns(inputTupleInfo),
                "ID, val, rowtime.rowtime"
        );

        Table intervalResult = tableEnv.sqlQuery("Select ID, sts, ets, intervalValue,valueDescription \n" +
                "From RawEvents\n" +
                "Match_Recognize(\n" +
                "PARTITION BY ID\n" +
                "ORDER BY rowtime\n" +
                "MEASURES\n" +
                "A.ID as id,\n" +
                "FIRST(A.rowtime) AS sts,\n" +
                "LAST(A.rowtime) AS ets,\n" +
                "AVG(A.val) as intervalValue,\n" +
                "'average' AS valueDescription\n" +
                "PATTERN ( A+ B)\n" +
                "DEFINE\n" +
                "A As A.val > 18,\n" +
                "B As true\n"+
                ")"
        );

        intervalResult.printSchema();

        TupleTypeInfo<Tuple5<String, Timestamp, Timestamp, Double, String>> tupleTypeInterval = new TupleTypeInfo<>(
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.SQL_TIMESTAMP(),
                Types.DOUBLE(),
                Types.STRING()
        );

        DataStream<Tuple5<String, Timestamp, Timestamp, Double, String>> queryResultAsStream = tableEnv.toAppendStream(intervalResult, tupleTypeInterval);

     //   queryResultAsStream.print();
      queryResultAsStream.map((MapFunction<Tuple5<String, Timestamp, Timestamp, Double, String>, IntervalEvent>) tuple -> new IntervalEvent(tuple.f1.getTime(), tuple.f2.getTime(), tuple.f3, tuple.f4, tuple.f0 )).print();

        env.execute("D2IA via Match Recognize");
    }
}
