package ee.ut.cs.dsg.example.linearroad.mapper;

import ee.ut.cs.dsg.example.linearroad.datagenerator.PerformanceFileBuilder;
import ee.ut.cs.dsg.example.linearroad.event.SpeedEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class SpeedMapper extends RichMapFunction<String, SpeedEvent> {

    private long counter = 0;
    private long startTime;
    private final String type;
    private final String expId;
    private PerformanceFileBuilder performanceFileBuilder;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.startTime = System.currentTimeMillis();
        super.open(parameters);
        this.performanceFileBuilder = new PerformanceFileBuilder("results-"+type+"-"+expId+"-"+this.getRuntimeContext().getTaskName()+"-"+this.getRuntimeContext().getIndexOfThisSubtask()+".csv","flink", this.getRuntimeContext().getExecutionConfig().getParallelism());
    }

    private void registerThroughput(){
        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        double throughput = (double)counter;
        double ddiff = (double) diff;
        throughput = throughput/diff;
        throughput = throughput *1000;
        performanceFileBuilder.register(type, throughput, expId, true, counter, diff);
    }

    public SpeedMapper(String type, String expId) {
        this.type=type;
        this.expId=expId;
    }

    @Override
    public SpeedEvent map(String s) throws Exception {
        //Schema of S is VID,SPEED,ACCEL,XWay,Lane,Dir,Seg,Pos,T1,T2
        String[] data = s.replace("[","").replace("]","").split(", ");
        counter++;
        if(counter%100==0)
            registerThroughput();

        return new SpeedEvent(data[0].trim(),Long.parseLong(data[8].trim()),Double.parseDouble(data[1].trim()));

    }
}

