package ee.ut.cs.dsg.example.linearroad.mapper;

import ee.ut.cs.dsg.example.linearroad.datagenerator.PerformanceFileBuilder;
import ee.ut.cs.dsg.example.linearroad.event.ExperimentConfiguration;
import ee.ut.cs.dsg.example.linearroad.event.SpeedEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class SpeedMapper extends RichMapFunction<String, SpeedEvent> {

    private int parallelism;
    private String implementation;
    private String query;
    private long startTime;
    private final String experimentId;
    private long eventsCounter=0;
    private long registerCounter=0;
    private long actualCounterRegistrationRate;
    private PerformanceFileBuilder performanceFileBuilder;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.startTime = System.currentTimeMillis();
        super.open(parameters);
        this.performanceFileBuilder = new PerformanceFileBuilder(ExperimentConfiguration.DEFAULT_PERFORMANCE_FILE_PATH+
                "performance-results-"+implementation+"-"+experimentId+"-"+query+"-parallelism_"+parallelism, "Flink");
    }

    private void registerThroughput(){
        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        double throughput = (double)eventsCounter;
        double ddiff = (double) diff;
        throughput = throughput/diff;
        throughput = throughput *1000;
        performanceFileBuilder.register(query, throughput, experimentId, true, eventsCounter, diff, startTime, endTime);
    }

    public SpeedMapper(String query, String expId) {
        this.query = query;
        this.experimentId =expId;
    }

    public SpeedMapper(String expId, String query, String implementation, int parallelism) {
        this.experimentId =expId;
        this.query = query;
        this.implementation = implementation;
        this.parallelism = parallelism;
        actualCounterRegistrationRate = ExperimentConfiguration.COUNTER_REGISTRATION_RATE_MINUTES*60*1000;
    }

    @Override
    public SpeedEvent map(String s) throws Exception {
        String[] data = s.replace("[","").replace("]","").split(", ");
        eventsCounter++;
        long currentTime = System.currentTimeMillis();
        if(registerCounter<(currentTime -this.startTime)/actualCounterRegistrationRate){
            registerCounter++;
            performanceFileBuilder.register(query, experimentId, startTime, currentTime, eventsCounter, implementation, parallelism);
        }
        if(System.currentTimeMillis()-startTime>30000)
            performanceFileBuilder.register(query, experimentId, startTime, System.currentTimeMillis(), eventsCounter, implementation, parallelism);

        return new SpeedEvent(data[0].trim(),Long.parseLong(data[8].trim()),Double.parseDouble(data[1].trim()));

    }

    @Override
    public void close() throws Exception {
        performanceFileBuilder.register(query, experimentId, startTime, System.currentTimeMillis(), eventsCounter, implementation, parallelism);
        super.close();
    }
}

