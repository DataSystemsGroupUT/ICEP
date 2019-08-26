package ee.ut.cs.dsg.example.source;

import ee.ut.cs.dsg.example.event.TemperatureEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class FixedSource implements SourceFunction<TemperatureEvent> {
    private boolean running = true;
//    private List<TemperatureEvent> myData;
//    public FixedSource (List<TemperatureEvent> data)
//    {
//        myData = new ArrayList<TemperatureEvent>(data.size());
//        myData.addAll(data);
//    }
    @Override
    public void run(SourceContext<TemperatureEvent> sourceContext) throws Exception {
        if (running)
        {


            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 1000, 20),1000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 2000, 19),2000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 8000, 18),8000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 9000, 21),9000);
//            sourceContext.emitWatermark(new Watermark(4));
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 5, 20),5);
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 6, 20),6);
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 7, 20),7);
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 8, 20),8);
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 9, 20),9);
            sourceContext.emitWatermark(new Watermark(10000));
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 10,20),10);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 11000,20),11000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 16000,17),16000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 17000,16),17000);
            sourceContext.emitWatermark(new Watermark(20000));

            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 25000, 17),25000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 24000, 17),24000);
            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 27000, 21),27000);
            sourceContext.emitWatermark(new Watermark(27));
//            sourceContext.collectWithTimestamp(new TemperatureEvent("1", 28, 19),28);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
