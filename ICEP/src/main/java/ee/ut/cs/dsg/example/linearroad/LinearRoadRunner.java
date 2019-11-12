package ee.ut.cs.dsg.example.linearroad;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.Operator;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import ee.ut.cs.dsg.d2ia.generator.HomogeneousIntervalGenerator;
import ee.ut.cs.dsg.example.linearroad.event.*;
import ee.ut.cs.dsg.example.linearroad.mapper.SpeedMapper;
import ee.ut.cs.dsg.example.linearroad.source.LinearRoadSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class LinearRoadRunner {

    static long windowLength=1L;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // I need to test the frequency of watermark generation

       // env.setMaxParallelism(16);
      //  env.setParallelism(16);
        ParameterTool parameters = ParameterTool.fromArgs(args);
        DataStream<SpeedEvent> rawEventStream;
        String source = parameters.getRequired("source");
        String kafka;
        String fileName;
        String topic;
        String jobType;
        String numRecordsToEmit;

        int iNumRecordsToEmit=Integer.MAX_VALUE;

        jobType = parameters.getRequired("jobType");

        String runAs;
        runAs = parameters.get("runAs");
        String generateOutput = parameters.get("generateOutput");
        if (generateOutput == null)
            generateOutput="No";
        if (runAs == null)
            runAs = "CEP";

        String timeMode = parameters.get("timeMode");
        if (timeMode == null)
            timeMode = "event";

        if (timeMode.equalsIgnoreCase("event"))
        {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            //env.getConfig().setAutoWatermarkInterval(10000L);
        }
        else
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String winLen = parameters.get("windowSize");

        if (winLen != null)
        {
            try
            {
                windowLength = Long.parseLong(winLen);
            }
            catch(Exception e)
            {

            }
        }

        numRecordsToEmit = parameters.get("numRecordsToEmit");

        if (numRecordsToEmit != null)
            iNumRecordsToEmit = Integer.parseInt(numRecordsToEmit);


        if (source.toLowerCase().equals("kafka")) {
            kafka = parameters.get("kafka");
            topic = parameters.get("topic");
//            zooKeeper = parameters.get("zookeeper");
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafka);
//            // only required for Kafka 0.8
//            properties.setProperty("zookeeper.connect", "localhost:2181");
//            properties.setProperty("group.id", "test");
            FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(),properties);
            consumer.setStartFromEarliest();
            rawEventStream = env.addSource(consumer).setParallelism(1).map(new SpeedMapper());
        } else {
            fileName = parameters.get("fileName");
            rawEventStream = env.addSource(new LinearRoadSource(fileName, iNumRecordsToEmit));//.setParallelism(1);
        }


        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
            rawEventStream = rawEventStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SpeedEvent>() {
                private long maxTimestampSeen = 0;

                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(maxTimestampSeen);
                }

                @Override
                public long extractTimestamp(SpeedEvent temperatureEvent, long l) {
                    long ts = temperatureEvent.getTimestamp();
                    // if (temperatureEvent.getKey().equals("W"))
                    maxTimestampSeen = Long.max(maxTimestampSeen, l);
                    return ts;
                }
            });//.setParallelism(1);
        }
    //    rawEventStream.writeAsText("C:\\Work\\Data\\lineartop"+iNumRecordsToEmit+".txt", FileSystem.WriteMode.OVERWRITE);

      // DataStream<String> rawEventStream = env.addSource(new LinearRoadSource("C:\\Work\\Data\\linear.csv", 100000));
        KeyedStream<SpeedEvent, String> speedStream = rawEventStream/*.filter((FilterFunction<SpeedEvent>) speedEvent -> speedEvent.getKey().equals("385"))*/.keyBy(RawEvent::getKey);//.setParallelism(1);
       // speedStream.writeAsText("c:\\Work\\Data\\FilterestedLinearRoad2-385.txt", FileSystem.WriteMode.OVERWRITE);
   //     speedStream.writeAsText("C:\\Work\\Data\\keyedStream"+iNumRecordsToEmit, FileSystem.WriteMode.OVERWRITE);
//                .filter((FilterFunction<SpeedEvent>) speedEvent -> speedEvent.getKey().equals("266")).setParallelism(1)
        ;
        //speedStream.writeAsText("c:\\Work\\Data\\FilterestedLinearRoad2", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        long start = System.currentTimeMillis();
        if (jobType.equals("ThresholdAbsolute"))
        {
            jobGenerateThresholdInterval(env,speedStream, runAs, generateOutput);
        }
        else if (jobType.equals("ThresholdRelative"))
        {
            jobGenerateThresholdIntervalWithRelativeCondition(env,speedStream, runAs, generateOutput);
        }
        else if (jobType.equals("Delta"))
        {
            jobGenerateDeltaIntervalWithRelativeCondition(env, speedStream, runAs, generateOutput);
        }
        else if (jobType.equals("Aggregate"))
        {
            jobGenerateAggregateIntervalWithRelativeCondition(env, speedStream, runAs, generateOutput);
        }





//        DataStream<AccelerationEvent> accelerationStream = rawEventStream.map(new AccelerationMapper());

        // Define Intervals

        // Threshold with absolute condition

//        runAs = "SQL";
//        jobGenerateThresholdInterval(env, speedStream, runAs);
        //jobGenerateThresholdIntervalWithRelativeCondition(env, speedStream, runAs);
        //jobGenerateAggregateIntervalWithRelativeCondition(env, speedStream, runAs);
//        jobGenerateDeltaIntervalWithRelativeCondition(env, speedStream, runAs);
        System.out.println("Time taken: "+(System.currentTimeMillis() - start) +" ms");
        //jobGenerateThresholdIntervalWithRelativeCondition(env, speedStream, runAs);
    }

    private static void jobGenerateThresholdInterval(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream, String runAs, String generateOutput) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedThresholdInterval> thresholdIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

     //   KeyedStream<SpeedEvent, String> keyedSpeedStream = speedStream.keyBy((KeySelector<SpeedEvent, String>) RawEvent::getKey);
       // keyedSpeedStream.print();
        thresholdIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .targetType(SpeedThresholdInterval.class)
                .produceOnlyMaximalIntervals(true)
            //    .within(Time.milliseconds(100))
                .minOccurrences(2)
             //   .maxOccurrences(5)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.GreaterThanEqual).RHS(50))
                .outputValue(Operand.Max);

        DataStream<SpeedThresholdInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithCEP();
        else if (runAs.equals("SQL"))
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithSQL(env);
        else
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithWindow(windowLength);

//        thresholdIntervalAbsoluteConditionDataStream.print();
        if (generateOutput.equalsIgnoreCase("yes"))
            thresholdIntervalAbsoluteConditionDataStream.writeAsText("./output-"+runAs+" parallelism "+env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
        env.execute("Linear road threshold interval with absolute condition run as "+runAs + " parallelism "+env.getParallelism() +" out of "+env.getMaxParallelism());
    }

    private static void jobGenerateThresholdIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream,
                                                                          String runAs,
                                                                          String generateOutput) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedThresholdInterval> thresholdIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

     //   KeyedStream<SpeedEvent, String> keyedSpeedStream = speedStream.keyBy((KeySelector<SpeedEvent, String>) RawEvent::getKey);
       // keyedSpeedStream.print().setParallelism(1);
        AbsoluteCondition cond1 = new AbsoluteCondition();
        AbsoluteCondition cond2 = new AbsoluteCondition();
        cond1.LHS(Operand.Last).operator(Operator.Multiply).RHS(0.1);
        cond2.LHS(Operand.Last).operator(Operator.Plus).RHS(cond1);
        thresholdIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                .produceOnlyMaximalIntervals(true)
                .targetType(SpeedThresholdInterval.class)
                .condition(new RelativeCondition().LHS(Operand.Value).operator(Operator.GreaterThanEqual).RHS(30)
                        .relativeLHS(Operand.Value).relativeOperator(Operator.GreaterThan).relativeRHS(cond2))
                .outputValue(Operand.Max);

        DataStream<SpeedThresholdInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithCEP();
        else if (runAs.equals("SQL"))
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithSQL(env);
        else
            thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.runWithWindow(windowLength);

//        thresholdIntervalAbsoluteConditionDataStream.print();
        if (generateOutput.equalsIgnoreCase("yes"))
            thresholdIntervalAbsoluteConditionDataStream.writeAsText("./output-Threshold-Relative-"+runAs+" parallelism "+env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
        //thresholdIntervalAbsoluteConditionDataStream.writeAsText("./output.txt");
        env.execute("Linear road threshold interval with relative condition run as "+runAs + " parallelism "+env.getParallelism() +" out of "+env.getMaxParallelism());
    }

    private static void jobGenerateAggregateIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream,
                                                                          String runAs,
                                                                          String generateOutput) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedAggregateInterval> aggregateWithRelativeCondition =
                new HomogeneousIntervalGenerator<>();

       // KeyedStream<SpeedEvent, String> keyedSpeedStream = speedStream.keyBy((KeySelector<SpeedEvent, String>) RawEvent::getKey);

        aggregateWithRelativeCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                .produceOnlyMaximalIntervals(true)
                // .within(Time.milliseconds(1000))
                .targetType(SpeedAggregateInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThanEqual).relativeRHS(67))
                .outputValue(Operand.Average);


        DataStream<SpeedAggregateInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = aggregateWithRelativeCondition.runWithCEP();
        else if (runAs.equals("SQL"))
            thresholdIntervalAbsoluteConditionDataStream = aggregateWithRelativeCondition.runWithSQL(env);
        else
            thresholdIntervalAbsoluteConditionDataStream = aggregateWithRelativeCondition.runWithWindow(windowLength);

       // thresholdIntervalAbsoluteConditionDataStream.print();
        if (generateOutput.equalsIgnoreCase("yes"))
            thresholdIntervalAbsoluteConditionDataStream.writeAsText("./output-Aggregate-"+runAs+" parallelism "+env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
        env.execute("Linear road aggregate interval with relative condition run as " + runAs + " parallelism "+env.getParallelism() +" out of "+env.getMaxParallelism());

    }

    private static void jobGenerateDeltaIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream,
                                                                      String runAs,
                                                                      String generateOutput) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedDeltaInterval> deltaIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

     //   KeyedStream<SpeedEvent, String> keyedSpeedStream = speedStream.keyBy((KeySelector<SpeedEvent, String>) RawEvent::getKey);

        AbsoluteCondition absoluteCondition = new AbsoluteCondition();
        absoluteCondition.operator(Operator.Absolute).RHS(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.Minus).RHS(Operand.First));

        deltaIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
               .minOccurrences(2)
               .produceOnlyMaximalIntervals(true)
//                .within(Time.milliseconds(10000))
                .targetType(SpeedDeltaInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(absoluteCondition).relativeOperator(Operator.GreaterThanEqual).relativeRHS(5))
                .outputValue(Operand.Average);


        DataStream<SpeedDeltaInterval> thresholdIntervalAbsoluteConditionDataStream;
        if (runAs.equals("CEP"))
            thresholdIntervalAbsoluteConditionDataStream = deltaIntervalWithAbsoluteCondition.runWithCEP();
        else if (runAs.equals("SQL"))
            thresholdIntervalAbsoluteConditionDataStream = deltaIntervalWithAbsoluteCondition.runWithSQL(env);
        else
            thresholdIntervalAbsoluteConditionDataStream = deltaIntervalWithAbsoluteCondition.runWithWindow(windowLength);

//        thresholdIntervalAbsoluteConditionDataStream.print();
        if (generateOutput.equalsIgnoreCase("yes"))
            thresholdIntervalAbsoluteConditionDataStream.writeAsText("./output-Delta-"+runAs+" parallelism "+env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
        env.execute("Linear road delta interval with relative condition run as " +runAs + " parallelism "+env.getParallelism() +" out of "+env.getMaxParallelism());

    }
}

