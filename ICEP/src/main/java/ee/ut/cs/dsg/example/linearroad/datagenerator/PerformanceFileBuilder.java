package ee.ut.cs.dsg.example.linearroad.datagenerator;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class PerformanceFileBuilder {

    private CSVWriter writer;
    private String platform;
    private int parallelism;

    public PerformanceFileBuilder(String fileName, String platform, int parallelism) {
        try {
            File file = new File(fileName);
            if(!file.exists()){
                this.writer = new CSVWriter(new FileWriter(file, false));
                String[] firstRow = new String[]{"Type", "Experiment-Name","Parallelism", "Platform", "Throughput", "OnCluster", "inputSize", "duration", "startTime", "endTime"};
                this.writer.writeNext(firstRow);
            }
            this.writer = new CSVWriter(new FileWriter(file, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.platform = platform;
        this.parallelism = parallelism;
    }

    public PerformanceFileBuilder(String fileName, String platform) {
        try {
            File file = new File(fileName);
            if(!file.exists()){
                file.createNewFile();
                this.writer = new CSVWriter(new FileWriter(file, false));
                String[] firstRow = new String[]{"Type", "Experiment-Name", "Platform", "OnCluster", "startTime", "currentTime", "eventsCount", "implementation", "parallelism"};
                this.writer.writeNext(firstRow);
                this.writer.flush();
            }
            this.writer = new CSVWriter(new FileWriter(file, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.platform = platform;
    }

    public void register(String expType, double throughput, String expName, boolean cluster, long inputSize){
        String[] row = new String[]{expType, expName, String.valueOf(parallelism), platform, String.valueOf(throughput), String.valueOf(cluster), String.valueOf(inputSize)};
        writer.writeNext(row);
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void register(String expType, double throughput, String expName, boolean cluster, long inputSize, long duration){
        String[] row = new String[]{expType, expName, String.valueOf(parallelism), platform, String.valueOf(throughput), String.valueOf(cluster), String.valueOf(inputSize), String.valueOf(duration)};
        writer.writeNext(row);
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void register(String expType, double throughput, String expName, boolean cluster, long inputSize, long duration, long startTime, long endTime){
        String[] row = new String[]{expType, expName, String.valueOf(parallelism), platform, String.valueOf(throughput), String.valueOf(cluster), String.valueOf(inputSize), String.valueOf(duration), String.valueOf(startTime), String.valueOf(endTime)};
        writer.writeNext(row);
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void register(String expType, String expName, long startTime, long currentTime, long eventsCount, String implementation, long parallelism){
        String[] row = new String[]{expType, expName, platform, String.valueOf(true), String.valueOf(startTime), String.valueOf(currentTime), String.valueOf(eventsCount), implementation, String.valueOf(parallelism)};
        System.out.println(Arrays.toString(row));
        writer.writeNext(row);
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
