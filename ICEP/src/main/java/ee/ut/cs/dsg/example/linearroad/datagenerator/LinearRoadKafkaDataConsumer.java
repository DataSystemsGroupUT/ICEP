package ee.ut.cs.dsg.example.linearroad.datagenerator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinearRoadKafkaDataConsumer {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaLinearRoadDataConsumersdjcas"+ UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("linear-road-data"));

            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(0L);
                records.forEach(record -> {
                    /*String[] data = record.value().replace("[","").replace("]","").split(",");
                    map.putIfAbsent(record.key(), record.partition());
                    if(record.partition() != map.get(record.key())){
                        wrong.set(true);
                        System.out.println("Something is wrong.");
                    }*/
                    //if(record.partition()==0)
                        System.out.println(record.value());

                });
            }
        } catch (WakeupException e) {
            // Using wakeup to close consumer
        } finally {
            consumer.close();
        }
    }
}
