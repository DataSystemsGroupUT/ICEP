/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.example.source;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import javax.security.auth.login.Configuration;

import ee.ut.cs.dsg.example.event.PowerEvent;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 *
 * @author MKamel
 */

public class PowerSource extends RichParallelSourceFunction<PowerEvent> {

    private final double powerStd;
    private final double powerMean;
    private boolean running = true;
    private final int maxRackId;
    private final long pause;

    public PowerSource(int maxRackId, long pause,  double powerStd, double powerMean) {
        this.maxRackId = maxRackId;
        this.pause = pause;
        this.powerMean = powerMean;
        this.powerStd = powerStd;
    }


    public void open(Configuration configuration) {
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int index = getRuntimeContext().getIndexOfThisSubtask();

        int offset = (int) ((double) maxRackId / numberTasks * index);
        int shard = (int) ((double) maxRackId / numberTasks * (index + 1)) - offset;

        Random random = new Random();
    }

    @Override
    public void run(SourceContext<PowerEvent> sourceContext) throws Exception {
        while (running) {
            String id = "1";
            PowerEvent powerevent;
            DateFormat dateFormat = new SimpleDateFormat("yyyy");
            Date dateFrom = dateFormat.parse("2017");
            long timestampFrom = dateFrom.getTime();
            Date dateTo = dateFormat.parse("2018");
            long timestampTo = dateTo.getTime();
            Random random = new Random();
            long timeRange = timestampTo - timestampFrom;
            long randomTimestamp = timestampFrom + (long) (random.nextDouble() * timeRange);

            double powervalue  = random.nextGaussian() * powerStd + powerMean;

            powerevent     = new PowerEvent(id, randomTimestamp, powervalue);
                sourceContext.collectWithTimestamp(powerevent, randomTimestamp);
                Thread.sleep(pause);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}