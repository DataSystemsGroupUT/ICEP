package ee.ut.cs.dsg.example.linearroad.datagenerator;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class LinearRoadKafkaDataProducer {

    private static final int NUMBER_OF_PARTITIONS=9;
    private static final Logger logger = Logger.getLogger(LinearRoadKafkaDataProducer.class.getName());

    private static Producer<Integer, String> createProducer(String[] args) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                args[0]);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaLinearRoadDataProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        //props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,4713360);
        //props.put(ProducerConfig.LINGER_MS_CONFIG,100);
        //props.put(ProducerConfig.RETRIES_CONFIG,5);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        //props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,90000);
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {


        final Producer<Integer, String> producer = createProducer(args);

        // Record to resume from : [1555, 47, 0.001, 0, 2, 1, 56, 296406, 377000, 377001]
        long time = System.currentTimeMillis();

        try {
            String fileName = args[2];
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            reader.readLine(); // this is the headerline
            line = reader.readLine();
            long nRecords = Long.parseLong(args[3]);
            long count = 0L;
            long actualTime = 0;
            while (line != null && (count<nRecords || nRecords==-1)) {
                count++;
                if(count%1000000==0)
                    logger.info("Sending "+count+"th record to Kafka");
                String[] lineDiv = line.replace("[","").replace("]","").split(", ");
                final ProducerRecord<Integer, String> record =
                        new ProducerRecord<>(args[1], Integer.parseInt(lineDiv[0].trim()), line);
                RecordMetadata metadata = producer.send(record).get();
                actualTime = Long.parseLong(lineDiv[8]);

                line = reader.readLine();
            }
            count++;

            for (int i = 0; i < 9; i++) {
                actualTime+=1000;
                line = "[-1, 0, 0.0, 0, 0, 0, 0, 0, "+actualTime+", "+(actualTime+1)+"]";
                final ProducerRecord<Integer, String> record =
                        new ProducerRecord<>(args[1], i, -1, line);
                logger.info("Sending "+count+"th record "+line +" to Kafka");
                producer.send(record);
                count++;

            }

            reader.close();
        } catch (IOException | InterruptedException | ExecutionException ioe) {
            ioe.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
