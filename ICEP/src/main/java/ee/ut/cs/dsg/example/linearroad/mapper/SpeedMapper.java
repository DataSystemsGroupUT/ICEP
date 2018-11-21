package ee.ut.cs.dsg.example.linearroad.mapper;

import ee.ut.cs.dsg.example.linearroad.event.SpeedEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class SpeedMapper implements MapFunction<String, SpeedEvent> {
    @Override
    public SpeedEvent map(String s) throws Exception {
        //Schema of S is VID,SPEED,ACCEL,XWay,Lane,Dir,Seg,Pos,T1,T2
        String[] data = s.split(",");
        SpeedEvent se = new SpeedEvent(data[0].trim(),Long.parseLong(data[8].trim()),Double.parseDouble(data[1].trim()));
        return se;

    }
}

