package ee.ut.cs.dsg.example.linearroad.event;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;

public class SpeedDeltaInterval extends IntervalEvent {
    public SpeedDeltaInterval(long sts, long ets, double value, String valueDescriptor, String k) {
        super(sts, ets, value, valueDescriptor, k);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpeedDeltaInterval) {
            SpeedDeltaInterval other = (SpeedDeltaInterval) obj;
            return value == other.getValue() && this.startTimestamp == other.getStartTimestamp() && this.endTimestamp == other.getEndTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return super.toString().replace("IntervalEvent","SpeedDeltaInterval");
    }
}
