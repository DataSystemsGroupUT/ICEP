package ee.ut.cs.dsg.example.linearroad.event;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

import static ee.ut.cs.dsg.example.linearroad.event.ExperimentConfiguration.COUNTER_REGISTRATION_RATE_MINUTES;

public class CustomStringSchema extends KafkaDeserializationSchemaWrapper<String> {

    private int nEndEvents;
    private int counter;
    private int registerCounter=0;
    private long maxTime;
    private long startTime;
    private long actualCounterRegistrationRate;


    public CustomStringSchema(DeserializationSchema<String> deserializationSchema, int nEndEvents) {
        super(deserializationSchema);
        this.nEndEvents = nEndEvents;
        this.counter=0;
        this.maxTime=0;
        this.actualCounterRegistrationRate = COUNTER_REGISTRATION_RATE_MINUTES*60*1000;

    }

    public CustomStringSchema(DeserializationSchema<String> deserializationSchema, long maxTimeMinutes) {
        super(deserializationSchema);
        this.maxTime = maxTimeMinutes*60*1000;
        this.startTime = System.currentTimeMillis();
        this.actualCounterRegistrationRate = COUNTER_REGISTRATION_RATE_MINUTES*60*1000;
        this.nEndEvents=-1;
    }

    public CustomStringSchema(DeserializationSchema<String> deserializationSchema, int nEndEvents, long maxTimeMinutes) {
        super(deserializationSchema);
        this.nEndEvents = nEndEvents;
        this.maxTime = maxTimeMinutes*60*1000;
        this.startTime = System.currentTimeMillis();
        this.actualCounterRegistrationRate = COUNTER_REGISTRATION_RATE_MINUTES*60*1000;
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        String[] data = nextElement.replace("[","").replace("]","").split(", ");

        if(Integer.parseInt(data[0])==-1)
            counter++;
        if(counter == nEndEvents || ((System.currentTimeMillis() - this.startTime > this.maxTime) && maxTime!=0))
            return true;
        return false;
    }

    public void setnEndEvents(int nEndEvents) {
        this.nEndEvents = nEndEvents;
    }
}
