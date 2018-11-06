/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.example;

import ee.ut.cs.dsg.example.event.PowerEvent;
import ee.ut.cs.dsg.example.event.PowerWarning;
import ee.ut.cs.dsg.example.event.TemperatureEvent;
import ee.ut.cs.dsg.example.event.TemperatureWarning;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.Operator;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.generator.HomogeneousIntervalGenerator;
import ee.ut.cs.dsg.d2ia.generator.IntervalOperator;
import ee.ut.cs.dsg.d2ia.generator.Match;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//import org.uniTartu.cep.interval2.events.TemperatureEvent;
//import org.uniTartu.cep.interval2.events.TemperatureWarning;
//import org.uniTartu.cep.interval2.sources.CEPIntervalSource;


import java.util.List;

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

        testHomogeneousIntervals();

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
                //   .within(Time.milliseconds(100))
                .outputValue(Operand.Max)
                .produceOnlyMaximalIntervals(true);

        DataStream<TemperatureWarning> warning1 = newInterval.run();

//        newInterval.source(inputEventStream)
//               .sourceType(TemperatureEvent.class)
//               .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(10))
//               .targetType(TemperatureWarning.class)
//               .outvalue(HomogeneousIntervalGenerator.Operator.Average)
//                .conditionType("R");
//               DataStream<TemperatureWarning> warning1= newInterval.run();
//        DataStream<TemperatureWarning> warning2   = newInterval.run_loop_generator(6);

//
//        newInterval.source(lineardata)
//                .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
//                .targetType(TemperatureWarning.class);
//        DataStream<TemperatureWarning> warning11= newInterval.run();
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
//                DataStream<TemperatureWarning> power1 = newInterval2.run();
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

//        env.readFile("F:\TPStream\linear_accel.events\linear_accel.events","F:\TPStream\linear_accel.events\linear_accel.events","F:\TPStream\linear_accel.events\linear_accel.events");

//        DataSet<Tuple2<String, String>> rawdata =
//                env.readCsvFile("E:\\CrimeReport.csv").includeFields("0000011").ignoreFirstLine()

        List<TemperatureEvent> myTemps = new ArrayList<TemperatureEvent>();

        myTemps.add(new TemperatureEvent("1", 1, 30));
        myTemps.add(new TemperatureEvent("1", 2, 35));
        myTemps.add(new TemperatureEvent("1", 3, 33));
        myTemps.add(new TemperatureEvent("1", 4, 40));
        DataStream<TemperatureEvent> inputEventStream = env
                .fromCollection(myTemps);


        List<PowerEvent> myPowers = new ArrayList<>();
        myPowers.add(new PowerEvent("1", 1, 30));
        myPowers.add(new PowerEvent("1", 2, 35));
        myPowers.add(new PowerEvent("1", 3, 33));
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


        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> newInterval = new HomogeneousIntervalGenerator<>();
        HomogeneousIntervalGenerator<PowerEvent, PowerWarning> newInterval2 = new HomogeneousIntervalGenerator<>();


        newInterval.source(inputEventStream)
                .sourceType(TemperatureEvent.class)
                //.condition(new AbsoluteCondition().operator(Operator.GreaterThan).RHS(30))
                .condition(new RelativeCondition().relativeLHS(Operand.Value).relativeOperator(Operator.GreaterThan).relativeRHS(Operand.Last).operator(Operator.GreaterThanEqual).RHS(30))
                //  .minOccurrences(2)
                .targetType(TemperatureWarning.class)
                //       .maxOccurrences(3)
                .within(Time.milliseconds(100))
                .outputValue(Operand.Max)
                .produceOnlyMaximalIntervals(true);

        DataStream<TemperatureWarning> warning1 = newInterval.run();

        newInterval2.source(inputEventStream2)
                .sourceType(PowerEvent.class)
                .condition(new RelativeCondition().relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThan).relativeRHS(30).operator(Operator.GreaterThanEqual).RHS(Operand.Value))
                .targetType(PowerWarning.class)
                .within(Time.milliseconds(100))
                .outputValue(Operand.Average)
                .produceOnlyMaximalIntervals(true);

        //               DataStream<TemperatureWarning> warning1= newInterval.run();

        DataStream<PowerWarning> warning2 = newInterval2.run();

//


//        inputEventStream2.print();
//
        warning1.print();
        warning2.print();

        IntervalOperator<TemperatureWarning,PowerWarning> matchOperator = new IntervalOperator<>();

        matchOperator.leftIntervalStream(warning1)
                .rightIntervalStream(warning2)
                .within(Time.milliseconds(100))
                .filterForMatchType(Match.MatchType.Equals)
                .filterForMatchType(Match.MatchType.During)
                .filterForMatchType(Match.MatchType.Contains)
                .filterForMatchType(Match.MatchType.Overlaps)
                .filterForMatchType(Match.MatchType.Starts)
                .filterForMatchType(Match.MatchType.StartedBy)
                .filterForMatchType(Match.MatchType.Finishes)
                .filterForMatchType(Match.MatchType.FinishedBy);

        DataStream<Match> matches = matchOperator.run();

        matches.print();

//        newInterval2.source(inputEventStream2)
//                .sourceType(PowerEvent.class)
//                .condition(1, 4, HomogeneousIntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
//                .targetType(TemperatureWarning.class)
//                .outvalue(HomogeneousIntervalGenerator.Operator.Last)
//                .conditionType("R");
//              //  .outvalue(HomogeneousIntervalGenerator.Operator.Average);
//                DataStream<TemperatureWarning> power1 = newInterval2.run();
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
}
