/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ut.ee.Sources;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import ut.ee.Events.PowerEvent;
import ut.ee.Events.TemperatureEvent;

/**
 *
 * @author MKamel
 */

public class PowerSource extends RichParallelSourceFunction<PowerEvent> {
    private static long time;

    private final double powerStd;
    private final double powerMean;
    private boolean running = true;
    private final int maxRackId;
    private final long pause;
    private Random random;
    private int shard;
    private int offset;
    double te = 0.0;


    public PowerSource(int maxRackId, long pause,  double powerStd, double powerMean) {
        this.maxRackId = maxRackId;
        this.pause = pause;
        this.powerMean = powerMean;
        this.powerStd = powerStd;
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
    public void run(SourceFunction.SourceContext<PowerEvent> sourceContext) throws Exception {
        while (running) {
            String id = "1";
            PowerEvent powerevent;

            double powevalue  = random.nextGaussian() * powerStd + powerMean;

            int rackId = random.nextInt(shard) + offset;
            DateFormat dateFormat = new SimpleDateFormat("yyyy");
            Date dateFrom = dateFormat.parse("2012");
            long timestampFrom = dateFrom.getTime();
            Date dateTo = dateFormat.parse("2013");
            long timestampTo = dateTo.getTime();
            Random random = new Random();
            long timeRange = timestampTo - timestampFrom;
            long randomTimestamp = timestampFrom + (long) (random.nextDouble() * timeRange);


            for (long i= 0; i<1000 ; i ++){

                time= i;
                te = te +50;
                powerevent     = new PowerEvent(rackId, time, te);
                sourceContext.collectWithTimestamp(powerevent, time);
                Thread.sleep(pause);

            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}

