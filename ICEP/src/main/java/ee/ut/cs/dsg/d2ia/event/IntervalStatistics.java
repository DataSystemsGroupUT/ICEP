package ee.ut.cs.dsg.d2ia.event;

import java.io.Serializable;

public class IntervalStatistics implements Serializable {
    public int count=0;
    public double sum = 0d;
    public double min = Double.MAX_VALUE;
    public double max = Double.MIN_VALUE;
    public double first = Double.MIN_VALUE;
    public double last = -1d;

    public void reset()
    {
        count=0;
        sum = 0d;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        first = Double.MIN_VALUE;
        last = -1d;
    }
}
