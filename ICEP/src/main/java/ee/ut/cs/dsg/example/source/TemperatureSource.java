/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.example.source;

import ee.ut.cs.dsg.example.event.TemperatureEvent;
import ut.ee.event.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;



import java.util.Date;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 *
 * @author MKamel
 */
public class TemperatureSource extends RichParallelSourceFunction<TemperatureEvent> {
    
    private boolean running = true;
    private final int maxRackId;
    private final long pause;
    private final double temperatureStd;
    private final double temperatureMean;
    private Random random;
    private int shard;
    private int offset;

    public TemperatureSource(
            int maxRackId,
            long pause,
            double temperatureStd,
            double temperatureMean) {
        this.maxRackId = maxRackId;
        this.pause = pause;
        this.temperatureMean = temperatureMean;
        this.temperatureStd = temperatureStd;
    }

    @Override
    public void open(Configuration configuration) {
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int index = getRuntimeContext().getIndexOfThisSubtask();
      
        offset = (int) ((double) maxRackId / numberTasks * index);
        shard = (int) ((double) maxRackId / numberTasks * (index + 1)) - offset;

        random = new Random();
    }

    @Override
    public void run(SourceFunction.SourceContext<TemperatureEvent> sourceContext) throws Exception {
        while (running) {
            String id = "1";
            TemperatureEvent temperaturevent;
                double temperature  = random.nextGaussian() * temperatureStd + temperatureMean;


            DateFormat dateFormat = new SimpleDateFormat("yyyy");
            Date dateFrom = dateFormat.parse("2012");
            long timestampFrom = dateFrom.getTime();
            Date dateTo = dateFormat.parse("2013");
            long timestampTo = dateTo.getTime();
            Random random = new Random();
            long timeRange = timestampTo - timestampFrom;
            long randomTimestamp = timestampFrom + (long) (random.nextDouble() * timeRange);
                temperaturevent     = new TemperatureEvent(id, randomTimestamp, temperature);

                sourceContext.collectWithTimestamp(temperaturevent, randomTimestamp);
                Thread.sleep(pause);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
    
}

