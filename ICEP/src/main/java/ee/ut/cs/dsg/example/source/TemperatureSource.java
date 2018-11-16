/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.example.source;

import ee.ut.cs.dsg.example.event.TemperatureEvent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;


import java.util.Date;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 * @author MKamel
 */
public class TemperatureSource implements SourceFunction<TemperatureEvent> {

    private boolean running = true;

    private final long pause;
    private final double temperatureStd;
    private final double temperatureMean;
    private Random random;

    public TemperatureSource(long pause, double temperatureStd, double temperatureMean) {
        this.pause = pause;
        this.temperatureMean = temperatureMean;
        this.temperatureStd = temperatureStd;
        random = new Random();
    }

//    @Override
//    public void open(Configuration configuration) {
//
//        random = new Random();
//    }

    @Override
    public void run(SourceFunction.SourceContext<TemperatureEvent> sourceContext) throws Exception {
        while (running) {
            String id = "1";
            TemperatureEvent temperatureEvent;
            double temperature = random.nextGaussian() * temperatureStd + temperatureMean;


//            DateFormat dateFormat = new SimpleDateFormat("yyyy");
//            Date dateFrom = dateFormat.parse("2012");
//            long timestampFrom = dateFrom.getTime();
//            Date dateTo = dateFormat.parse("2013");
//            long timestampTo = dateTo.getTime();
//            Random random = new Random();
//            long timeRange = timestampTo - timestampFrom;
//            long randomTimestamp = timestampFrom + (long) (random.nextDouble() * timeRange);
            long timestamp = System.currentTimeMillis();
            temperatureEvent = new TemperatureEvent(id, timestamp, temperature);

            sourceContext.collectWithTimestamp(temperatureEvent, timestamp);
            Thread.sleep(pause);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}

