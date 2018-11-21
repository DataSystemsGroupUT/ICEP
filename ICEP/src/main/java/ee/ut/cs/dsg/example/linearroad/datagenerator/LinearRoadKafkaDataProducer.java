package ee.ut.cs.dsg.example.linearroad.datagenerator;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class LinearRoadKafkaDataProducer {
    private static String topic = "linear-road-data";
    private static String bootstrapServers = "kafka-node-01:9092,kafka-node-02:9092,kafka-node-03:9092";
    private static String fileName;

    public static void setInputFile(String inputFileName) {
        fileName = inputFileName;
    }

    public static void setKafkaBroker(String host, String port)
    {
        bootstrapServers = host+":"+port;
    }
    public static void setKafkaTopic(String topiK)
    {
        topic = topiK;
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaLinearRoadDataProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,4713360);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.LINGER_MS_CONFIG,100);
        props.put(ProducerConfig.RETRIES_CONFIG,5);

        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,90000);
        return new KafkaProducer<>(props);
    }

    public static void runProducer() throws Exception {
        final Producer<String, String> producer = createProducer();

        // Record to resume from : [1555, 47, 0.001, 0, 2, 1, 56, 296406, 377000, 377001]
        long time = System.currentTimeMillis();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            reader.readLine(); // this is the headerline
            line = reader.readLine();
            while (line != null) {

                System.out.println("Sending record "+line +" to Kafka");

                final ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, 0,"Dummy", line);
                RecordMetadata metadata = producer.send(record).get();

                line = reader.readLine();
            }
            reader.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws Exception
    {
        setInputFile("C:\\Work\\Data\\linear2.csv");
      //  setKafkaBroker("172.17.77.48", "9092");
        setKafkaTopic("linear-road-data");
        runProducer();
    }
}
