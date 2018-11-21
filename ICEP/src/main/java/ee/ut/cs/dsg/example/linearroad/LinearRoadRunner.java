package ee.ut.cs.dsg.example.linearroad;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.Operator;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.generator.HomogeneousIntervalGenerator;
import ee.ut.cs.dsg.example.linearroad.event.*;
import ee.ut.cs.dsg.example.linearroad.mapper.AccelerationMapper;
import ee.ut.cs.dsg.example.linearroad.mapper.SpeedMapper;
import ee.ut.cs.dsg.example.linearroad.source.LinearRoadSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class LinearRoadRunner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        DataStream<String> stringStream;
        String source = parameters.getRequired("source");
        String kafka;
        String fileName;
        String topic;
        String jobType;
        jobType = parameters.getRequired("jobType");

        if (source.toLowerCase().equals("kafka")) {
            kafka = parameters.get("kafka");
            topic = parameters.get("topic");
//            zooKeeper = parameters.get("zookeeper");
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafka);
//            // only required for Kafka 0.8
//            properties.setProperty("zookeeper.connect", "localhost:2181");
//            properties.setProperty("group.id", "test");
            FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(),properties);
            consumer.setStartFromEarliest();
            stringStream = env.addSource(consumer);
        } else {
            fileName = parameters.get("fileName");
            stringStream = env.addSource(new LinearRoadSource(fileName));
        }

        DataStream<SpeedEvent> speedStream = stringStream.map(new SpeedMapper());
        if (jobType.equals("ThresholdAbsolute"))
        {
            jobGenerateThresholdInterval(env,speedStream);
        }
        else if (jobType.equals("ThresholdRelative"))
        {
            jobGenerateThresholdIntervalWithRelativeCondition(env,speedStream);
        }
        else if (jobType.equals("Delta"))
        {
            jobGenerateDeltaIntervalWithRelativeCondition(env, speedStream);
        }
        else if (jobType.equals("Aggregate"))
        {
            jobGenerateAggregateIntervalWithRelativeCondition(env, speedStream);
        }

//        DataStream<String> stringStream = env.addSource(new LinearRoadSource("C:\\Work\\Data\\linear.csv"));



//        DataStream<AccelerationEvent> accelerationStream = stringStream.map(new AccelerationMapper());

        // Define Intervals

        // Threshold with absolute condition
        // jobGenerateThresholdInterval(env, speedStream);
        jobGenerateThresholdIntervalWithRelativeCondition(env, speedStream);
    }

    private static void jobGenerateThresholdInterval(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedThresholdInterval> thresholdIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();

        thresholdIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .targetType(SpeedThresholdInterval.class)
                .condition(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.GreaterThanEqual).RHS(30))
                .outputValue(Operand.Max);

        DataStream<SpeedThresholdInterval> thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.run();

        thresholdIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road threshold interval with absolute condition");
    }

    private static void jobGenerateThresholdIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream) throws Exception {
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

        DataStream<SpeedThresholdInterval> thresholdIntervalAbsoluteConditionDataStream = thresholdIntervalWithAbsoluteCondition.run();

        thresholdIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road threshold interval with relative condition");
    }

    private static void jobGenerateAggregateIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedDeltaInterval> deltaIntervalWithRelativeCondition =
                new HomogeneousIntervalGenerator<>();

        AbsoluteCondition absoluteCondition = new AbsoluteCondition();
        absoluteCondition.operator(Operator.Absolute).RHS(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.Minus).RHS(Operand.Min));

        deltaIntervalWithRelativeCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                .within(Time.milliseconds(1000))
                .targetType(SpeedDeltaInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(absoluteCondition).relativeOperator(Operator.GreaterThanEqual).relativeRHS(2))
                .outputValue(Operand.Average);


        DataStream<SpeedDeltaInterval> deltaIntervalAbsoluteConditionDataStream = deltaIntervalWithRelativeCondition.run();

        deltaIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road delta interval with relative condition");
    }

    private static void jobGenerateDeltaIntervalWithRelativeCondition(StreamExecutionEnvironment env, DataStream<SpeedEvent> speedStream) throws Exception {
        HomogeneousIntervalGenerator<SpeedEvent, SpeedDeltaInterval> deltaIntervalWithAbsoluteCondition =
                new HomogeneousIntervalGenerator<>();


        deltaIntervalWithAbsoluteCondition.sourceType(SpeedEvent.class)
                .source(speedStream)
                .minOccurrences(2)
                .within(Time.milliseconds(1000))
                .targetType(SpeedAggregateInterval.class)
                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThan).relativeRHS(36))
                .outputValue(Operand.Average);


        DataStream<SpeedDeltaInterval> aggregateIntervalAbsoluteConditionDataStream = deltaIntervalWithAbsoluteCondition.run();

        aggregateIntervalAbsoluteConditionDataStream.print();
        env.execute("Linear road aggregate interval with relative condition");
    }
}
