/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ut.ee.Main;

import ut.ee.icep.events.*;
import ut.ee.icep.generator.*;
import ut.ee.Sources.*;
import ut.ee.Events.*;

import java.util.ArrayList;

import jdk.nashorn.internal.runtime.regexp.joni.Warnings;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

//import org.uniTartu.cep.interval2.events.TemperatureEvent;
//import org.uniTartu.cep.interval2.events.TemperatureWarning;
//import org.uniTartu.cep.interval2.sources.CEPIntervalSource;


import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;

//import static java.util.regex.Pattern.union;
import static java.awt.SystemColor.window;
import static javafx.scene.input.KeyCode.T;
import static javafx.scene.input.KeyCode.getKeyCode;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;


/**
 *
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
        
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.readFile("F:\TPStream\linear_accel.events\linear_accel.events","F:\TPStream\linear_accel.events\linear_accel.events","F:\TPStream\linear_accel.events\linear_accel.events");

//        DataSet<Tuple2<String, String>> rawdata =
//                env.readCsvFile("E:\\CrimeReport.csv").includeFields("0000011").ignoreFirstLine()

        DataStream<TemperatureEvent> inputEventStream = env
                .addSource(new TemperatureSource(
                        MAX_RACK_ID,
                        PAUSE,
                        TEMP_STD,
                        TEMP_MEAN))
               .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        DataStream<String> lineardata = env.readTextFile("F:\\TPStream\\linear_accel.events\\linear_accel.events");



//        StreamTableSource<TemperatureEvent> extends TableSource<TemperatureWarning> {
//
//            public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
//        }




        DataStream<PowerEvent> inputEventStream2 = env
                .addSource(new PowerSource(
                        MAX_RACK_ID,
                        PAUSE,
                        power_STD,
                        power_MEAN))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());


       IntervalGenerator<TemperatureEvent, TemperatureWarning> newInterval = new IntervalGenerator<>();
       IntervalGenerator<PowerEvent, TemperatureWarning> newInterval2 = new IntervalGenerator<>();



        newInterval.source(inputEventStream)
               .sourceType(TemperatureEvent.class)
               .condition(1, 4, IntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(10))
               .targetType(TemperatureWarning.class)
               .outvalue(IntervalGenerator.Operator.Average)
                .conditionType("R");
               DataStream<TemperatureWarning> warning1= newInterval.run_generator();
        DataStream<TemperatureWarning> warning2   = newInterval.run_loop_generator(6);

//
//        newInterval.source(lineardata)
//                .condition(1, 4, IntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
//                .targetType(TemperatureWarning.class);
//        DataStream<TemperatureWarning> warning11= newInterval.run_generator();
//        DataStream<TemperatureWarning> warning22   = newInterval.run_loop_generator(4);
//



        warning1.print();
//        warning2.print();




        newInterval2.source(inputEventStream2)
                .sourceType(PowerEvent.class)
                .condition(1, 4, IntervalGenerator.Operator.GreaterThanEqual, TEMPERATURE_THRESHOLD, Time.minutes(1))
                .targetType(TemperatureWarning.class)
                .outvalue(IntervalGenerator.Operator.Last)
                .conditionType("R");
              //  .outvalue(IntervalGenerator.Operator.Average);
                DataStream<TemperatureWarning> power1 = newInterval2.run_generator();
        DataStream<TemperatureWarning> power2 = newInterval2.run_loop_generator(4);
//        power1.print();
//        power2.print();


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
