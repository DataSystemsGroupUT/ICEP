package ee.ut.cs.dsg.example.source;

import ee.ut.cs.dsg.example.event.TemperatureEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class FixedSource implements SourceFunction<TemperatureEvent> {
    private boolean running = true;
    @Override
    public void run(SourceContext<TemperatureEvent> sourceContext) throws Exception {
        if (running)
        {
//            myTemps.add(new TemperatureEvent("1", 1, 20));
//            myTemps.add(new TemperatureEvent("1", 2, 20));
//            myTemps.add(new TemperatureEvent("1", 3, 20));
//            myTemps.add(new TemperatureEvent("1", 4, 21));
//            myTemps.add(new TemperatureEvent("W", 4, 21));
//            myTemps.add(new TemperatureEvent("1", 5, 20));
//            myTemps.add(new TemperatureEvent("1", 6, 20));
//            myTemps.add(new TemperatureEvent("1", 7, 20));
//            myTemps.add(new TemperatureEvent("1", 8, 20));
//            myTemps.add(new TemperatureEvent("1", 9, 20));
//            myTemps.add(new TemperatureEvent("W", 9, 21));
//            myTemps.add(new TemperatureEvent("1", 10, 20));
//            myTemps.add(new TemperatureEvent("1", 11, 20));
//            myTemps.add(new TemperatureEvent("1", 12, 20));
//            myTemps.add(new TemperatureEvent("1", 13, 20));
//            myTemps.add(new TemperatureEvent("W", 14, 21));

            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 1, 20),1);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 2, 20),2);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 3, 20),3);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 4, 21),4);
            sourceContext.emitWatermark(new Watermark(4));
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 5, 20),5);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 6, 20),6);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 7, 20),7);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 8, 20),8);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 9, 20),9);
            sourceContext.emitWatermark(new Watermark(9));
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 10,20),10);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 11,20),11);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 12,20),12);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 13,20),13);
            sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));

//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 25, 17),25);
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 24, 17),24);
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 27, 21),27);
//            sourceContext.emitWatermark(new Watermark(27));
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 28, 19),28);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
