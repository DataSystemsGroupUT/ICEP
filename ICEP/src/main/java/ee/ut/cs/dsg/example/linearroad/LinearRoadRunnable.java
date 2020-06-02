package ee.ut.cs.dsg.example.linearroad;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.runtime.client.EPEventService;
import ee.ut.cs.dsg.example.linearroad.event.SpeedEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class LinearRoadRunnable implements Runnable {

    private static final long serialVersionUID = -2873892890991630938L;
    private final EventSender sender;
    private final EPEventService service;
    private boolean running = true;
    private String filePath;
    private int numRecordsToEmit = Integer.MAX_VALUE;

    public LinearRoadRunnable(String filePath, EPEventService service, String type) {
        this.filePath = filePath;
        this.sender = service.getEventSender(type);
        this.service = service;
    }

    public LinearRoadRunnable(String filePath, int numRecordsToEmit, EPEventService service, String type) {
        this.filePath = filePath;
        this.sender = service.getEventSender(type);
        this.service = service;
        this.filePath = filePath;
        this.numRecordsToEmit = numRecordsToEmit;
    }

    @Override
    public void run() {
        try {
            int recordsEmitted = 0;
            BufferedReader reader;
            if (filePath.startsWith("http")) {
                URL url = new URL(filePath);
                InputStreamReader is = new InputStreamReader(url.openStream());

//            BufferedReader reader = new BufferedReader(new FileReader(filePath));
                reader = new BufferedReader(is);
            } else {
                reader = new BufferedReader(new FileReader(filePath));
            }
            String line;
            reader.readLine();//skip the header line
            line = reader.readLine();
//            List<String> uniqueKeys = new ArrayList<>();
            while (running && line != null && recordsEmitted <= numRecordsToEmit) {
                String[] data = line.replace("[", "").replace("]", "").split(",");

//                sourceContext.collect(new SpeedEvent(data[0].trim(),Long.parseLong(data[8].trim()),Double.parseDouble(data[1].trim())));
                Long ts = Long.parseLong(data[8].trim());
//                if (!uniqueKeys.contains(data[0].trim()))
//                    uniqueKeys.add(data[0].trim());
                if (recordsEmitted == numRecordsToEmit) {
//                    sourceContext.collectWithTimestamp(new SpeedEvent(data[0].trim(),ts,Double.parseDouble(data[1].trim())),Long.MAX_VALUE);
                    send(data, ts);
//                    for (String key: uniqueKeys)
//                        sourceContext.collectWithTimestamp(new SpeedEvent(key, Long.MAX_VALUE, new Double(-100)), Long.MAX_VALUE);
//                    break;
                } else {
                    send(data, ts);
                }


                //        sourceContext.emitWatermark(new Watermark(ts));
                recordsEmitted++;
                line = reader.readLine();
            }
            reader.close();
//            for (String key: uniqueKeys)
////                        sourceContext.collectWithTimestamp(new SpeedEvent(key, Long.MAX_VALUE, new Double(-100)), Long.MAX_VALUE);
            service.advanceTime(Long.MAX_VALUE);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void send(String[] data, Long ts) {
        if (service.getCurrentTime() < ts) {
            service.advanceTime(ts);
        }
        sender.sendEvent(new SpeedEvent(data[0].trim(), ts, Double.parseDouble(data[1].trim())));
    }

}
