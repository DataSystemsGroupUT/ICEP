package ee.ut.cs.dsg.example.linearroad.event;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CustomStringSchema extends KafkaDeserializationSchemaWrapper<String> {
    private int nEndEvents;
    private int counter;


    public CustomStringSchema(DeserializationSchema<String> deserializationSchema, int nEndEvents) {
        super(deserializationSchema);
        this.nEndEvents = nEndEvents;
        this.counter=0;
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        String[] data = nextElement.replace("[","").replace("]","").split(", ");
        if(Integer.parseInt(data[0])==-1)
            counter++;
        return counter == nEndEvents;
    }

    public void setnEndEvents(int nEndEvents) {
        this.nEndEvents = nEndEvents;
    }
}
