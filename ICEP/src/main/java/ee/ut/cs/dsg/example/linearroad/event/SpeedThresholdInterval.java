package ee.ut.cs.dsg.example.linearroad.event;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;

public class SpeedThresholdInterval extends IntervalEvent {
    public SpeedThresholdInterval(){}
    public SpeedThresholdInterval(long sts, long ets, double value, String valueDescriptor, String k) {
        super(sts, ets, value, valueDescriptor, k);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpeedThresholdInterval) {
            SpeedThresholdInterval other = (SpeedThresholdInterval) obj;
            return getValue() == other.getValue() && this.getStartTimestamp() == other.getStartTimestamp()
                    && this.getEndTimestamp() == other.getEndTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return super.toString().replace("IntervalEvent","SpeedThresholdInterval");
    }
}
