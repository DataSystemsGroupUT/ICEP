package ee.ut.cs.dsg.example.linearroad.mapper;

import ee.ut.cs.dsg.example.linearroad.event.AccelerationEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class AccelerationMapper implements MapFunction<String, AccelerationEvent> {
    @Override
    public AccelerationEvent map(String s) throws Exception {
        //Schema of S is VID,SPEED,ACCEL,XWay,Lane,Dir,Seg,Pos,T1,T2
        String[] data = s.split(",");
        return new AccelerationEvent(data[0].trim(),
                Long.parseLong(data[8].trim()),
                Double.parseDouble(data[2].trim()));

    }
}

