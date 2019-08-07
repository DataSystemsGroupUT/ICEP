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
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 1, 10),1);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 2, 12),2);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 3, 15),3);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 5, 20),5);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 6, 21),6);
            sourceContext.emitWatermark(new Watermark(6));
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 7, 19),7);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 4, 21),4);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 8, 17),8);
            sourceContext.emitWatermark(new Watermark(24));
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 25, 17),25);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 24, 17),24);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 27, 21),27);
            sourceContext.emitWatermark(new Watermark(27));
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 28, 19),28);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
