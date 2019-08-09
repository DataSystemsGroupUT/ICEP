package ee.ut.cs.dsg.example.linearroad;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.Operator;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.generator.HomogeneousIntervalGenerator;
import ee.ut.cs.dsg.example.linearroad.event.*;
import ee.ut.cs.dsg.example.linearroad.mapper.SpeedMapper;
import ee.ut.cs.dsg.example.linearroad.source.LinearRoadSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import javax.annotation.Nullable;
import java.util.Properties;

public class LinearRoadRunner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10);
//        ParameterTool parameters = ParameterTool.fromArgs(args);
//        DataStream<String> stringStream;
//        String source = parameters.getRequired("source");
//        String kafka;
//        String fileName;
//        String topic;
//        String jobType;
//
//        jobType = parameters.getRequired("jobType");

        String runAs;
//        runAs = parameters.get("runAs");
//        if (runAs == null)
            runAs = "CEP";

//        if (source.toLowerCase().equals("kafka")) {
//            kafka = parameters.get("kafka");
//            topic = parameters.get("topic");
////            zooKeeper = parameters.get("zookeeper");
//            Properties properties = new Properties();
//            properties.setProperty("bootstrap.servers", kafka);
////            // only required for Kafka 0.8
////            properties.setProperty("zookeeper.connect", "localhost:2181");
////            properties.setProperty("group.id", "test");
//            FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(),properties);
//            consumer.setStartFromEarliest();
//            stringStream = env.addSource(consumer);
//        } else {
//            fileName = parameters.get("fileName");
//            stringStream = env.addSource(new LinearRoadSource(fileName));
//        }
       DataStream<String> stringStream = env.addSource(new LinearRoadSource("C:\\Work\\Data\\linear.csv"));
        DataStream<SpeedEvent> speedStream = stringStream.map(new SpeedMapper());
        speedStream = speedStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SpeedEvent>() {
            private long maxTimestampSeen = 0;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTimestampSeen);
            }

            @Override
            public long extractTimestamp(SpeedEvent temperatureEvent, long l) {
                long ts = temperatureEvent.getTimestamp();
               // if (temperatureEvent.getKey().equals("W"))
                    maxTimestampSeen = Long.max(maxTimestampSeen,ts);
                return ts;
            }
        }).filter(new FilterFunction<SpeedEvent>() {
            @Override
            public boolean filter(SpeedEvent speedEvent) throws Exception {
                return speedEvent.getKey().equals("266");
            }
        });
        speedStream.writeAsText("c:\\Work\\Data\\FilterestedLinearRoad2", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
//        if (jobType.equals("ThresholdAbsolute"))
//        {
//            jobGenerateThresholdInterval(env,speedStream, runAs);
//        }
//        else if (jobType.equals("ThresholdRelative"))
//        {
//            jobGenerateThresholdIntervalWithRelativeCondition(env,speedStream, runAs);
//        }
//        else if (jobType.equals("Delta"))
//        {
//            jobGenerateDeltaIntervalWithRelativeCondition(env, speedStream, runAs);
//        }
//        else if (jobType.equals("Aggregate"))
//        {
//            jobGenerateAggregateIntervalWithRelativeCondition(env, speedStream, runAs);
//        }





//        DataStream<AccelerationEvent> accelerationStream = stringStream.map(new AccelerationMapper());

        // Define Intervals

        // Threshold with absolute condition
        runAs = "globalWindow";
        jobGenerateThresholdInterval(env, speedStream, runAs);
        //jobGenerateThresholdIntervalWithRelativeCondition(env, speedStream, runAs);
    }

    private static void jobGenerateThresholdInterval(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream, String runAs) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedThresholdInterval> thresholdIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

        KeyedStream<SpeedEvent, String> keyedSpeedStream = speedStream.keyBy(new KeySelector<SpeedEvent, String>() {
            @Override
            public String getKey(SpeedEvent value) throws Exception {

                return value.getKey();

            }
        });
        thresholdIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(keyedSpeedStream)
                .targetType(SpeedThresholdInterval.class)
            //    .produceOnlyMaximalIntervals(true)
            //    .within(Time.milliseconds(100))
                //.minOccurrences(2)
             //   .maxOccurrences(5)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.GreaterThanEqual).RHS(20))
                .outputValue(Operand.Max);

        DataStream<SpeedThresholdInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithCEP();
        else
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithGlobalWindow();

        thresholdIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road threshold interval with absolute condition");
    }

    private static void jobGenerateThresholdIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream, String runAs) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedThresholdInterval> thresholdIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

        AbsoluteCondition cond1 = new AbsoluteCondition();
        AbsoluteCondition cond2 = new AbsoluteCondition();
        cond1.LHS(Operand.Last).operator(Operator.Multiply).RHS(0.1);
        cond2.LHS(Operand.Last).operator(Operator.Plus).RHS(cond1);
        thresholdIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                .targetType(SpeedThresholdInterval.class)
                .condition(new RelativeCondition().LHS(Operand.Value).operator(Operator.GreaterThanEqual).RHS(30)
                        .relativeLHS(Operand.Value).relativeOperator(Operator.GreaterThan).relativeRHS(cond2))
                .outputValue(Operand.Max);

        DataStream<SpeedThresholdInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithCEP();
        else
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithGlobalWindow();

        thresholdIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road threshold interval with relative condition");
    }

    private static void jobGenerateAggregateIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream, String runAs) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedAggregateInterval> aggregateWithRelativeCondition =
                new HomogeneousIntervalGenerator<>();



        aggregateWithRelativeCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                // .within(Time.milliseconds(1000))
                .targetType(SpeedAggregateInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThanEqual).relativeRHS(75))
                .outputValue(Operand.Average);


        DataStream<SpeedAggregateInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = aggregateWithRelativeCondition.runWithCEP();
        else
            thresholdIntervalAbsoluteConditionDataStream = aggregateWithRelativeCondition.runWithGlobalWindow();

        thresholdIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road aggregate interval with relative condition");
    }

    private static void jobGenerateDeltaIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream, String runAs) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedDeltaInterval> deltaIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

        AbsoluteCondition absoluteCondition = new AbsoluteCondition();
        absoluteCondition.operator(Operator.Absolute).RHS(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.Minus).RHS(Operand.Min));

        deltaIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                //   .within(Time.milliseconds(1000))
                .targetType(SpeedDeltaInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(absoluteCondition).relativeOperator(Operator.GreaterThanEqual).relativeRHS(30))
                .outputValue(Operand.Average);


        DataStream<SpeedDeltaInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = deltaIntervalWithAbsoluteCondition.runWithCEP();
        else
            thresholdIntervalAbsoluteConditionDataStream = deltaIntervalWithAbsoluteCondition.runWithGlobalWindow();

        thresholdIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road delta interval with relative condition");
    }
}

