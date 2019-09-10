package ee.ut.cs.dsg.example.linearroad.source;

import ee.ut.cs.dsg.example.linearroad.event.SpeedEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class LinearRoadSource implements SourceFunction<SpeedEvent> {

    private static final long serialVersionUID = -2873892890991630938L;
    private boolean running = true;
    private String filePath;
    private int numRecordsToEmit=Integer.MAX_VALUE;
    public LinearRoadSource(String filePath) {
        this.filePath = filePath;
    }

    public LinearRoadSource(String filePath, int numRecordsToEmit)
    {
        this.filePath = filePath;
        this.numRecordsToEmit = numRecordsToEmit;
    }

    @Override
    public void run(SourceContext<SpeedEvent> sourceContext) throws Exception {
        try {
            int recordsEmitted=0;
            BufferedReader reader;
            if (filePath.startsWith("http")) {
                URL url = new URL(filePath);
                InputStreamReader is = new InputStreamReader(url.openStream());

//            BufferedReader reader = new BufferedReader(new FileReader(filePath));
                reader = new BufferedReader(is);
            }
            else
            {
                reader = new BufferedReader(new FileReader(filePath));
            }
            String line;
            line = reader.readLine();//skip the header line
            line = reader.readLine();
            List<String> uniqueKeys = new ArrayList<>();
            while (running && line != null && recordsEmitted <= numRecordsToEmit) {
                String[] data = line.replace("[","").replace("]","").split(",");

//                sourceContext.collect(new SpeedEvent(data[0].trim(),Long.parseLong(data[8].trim()),Double.parseDouble(data[1].trim())));
                Long ts = Long.parseLong(data[8].trim());
                if (!uniqueKeys.contains(data[0].trim()))
                    uniqueKeys.add(data[0].trim());
                if (recordsEmitted==numRecordsToEmit)
                {
                    sourceContext.collectWithTimestamp(new SpeedEvent(data[0].trim(),ts,Double.parseDouble(data[1].trim())),Long.MAX_VALUE);
                    for (String key: uniqueKeys)
                        sourceContext.collectWithTimestamp(new SpeedEvent(key, Long.MAX_VALUE, new Double(-100)), Long.MAX_VALUE);
                    break;
                }else
                    sourceContext.collectWithTimestamp(new SpeedEvent(data[0].trim(),ts,Double.parseDouble(data[1].trim())),ts);

        //        sourceContext.emitWatermark(new Watermark(ts));
                recordsEmitted++;
                line=reader.readLine();
            }
            reader.close();
          //  sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}
