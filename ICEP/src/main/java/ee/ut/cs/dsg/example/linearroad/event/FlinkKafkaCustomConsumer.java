package ee.ut.cs.dsg.example.linearroad.event;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkKafkaCustomConsumer<T> extends FlinkKafkaConsumer011<T> {

    private static final long serialVersionUID = 2324564345203409112L;
    public FlinkKafkaCustomConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(topic, deserializer, props);
    }



    @Override
    public AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {

        ((CustomStringSchema)deserializer).setnEndEvents(assignedPartitionsWithInitialOffsets.keySet().size());

        return super.createFetcher(sourceContext, assignedPartitionsWithInitialOffsets, watermarksPeriodic, watermarksPunctuated, runtimeContext, offsetCommitMode, consumerMetricGroup, useMetrics);
    }

}
