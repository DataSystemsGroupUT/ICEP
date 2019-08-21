package ee.ut.cs.dsg.example.linearroad.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.net.URL;

public class LinearRoadSource implements SourceFunction<String> {

    private static final long serialVersionUID = -2873892890991630938L;
    private boolean running = true;
    private String filePath;

    public LinearRoadSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        try {
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
            while (running && line != null) {
                sourceContext.collect(line.replace("[","").replace("]",""));
                line=reader.readLine();
            }
            reader.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
