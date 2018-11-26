package ee.ut.cs.dsg.example.linearroad.event;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;

public class SpeedThresholdInterval extends IntervalEvent {
    public SpeedThresholdInterval(long sts, long ets, double value, String valueDescriptor, String k) {
        super(sts, ets, value, valueDescriptor, k);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpeedThresholdInterval) {
            SpeedThresholdInterval other = (SpeedThresholdInterval) obj;
            return value == other.getValue() && this.startTimestamp == other.getStartTimestamp() && this.endTimestamp == other.getEndTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public String toString()
    {
        String s = super.toString().replace("IntervalEvent","SpeedThresholdInterval");
        return s;
    }
}