package ee.ut.cs.dsg.example.linearroad.event;

import ee.ut.cs.dsg.d2ia.event.RawEvent;

public class AccelerationEvent extends RawEvent {
    public AccelerationEvent(){}
    public AccelerationEvent(String k, long ts, double v) {
        super(k, ts, v);
    }
}
