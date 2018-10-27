/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ut.ee.icep.events;

/**
 *
 * @author MKamel
 */

public class IntervalEvent {

    protected long startTimestamp;
    protected long endTimestamp;
    protected double value;

    public IntervalEvent(long sts, long ets, double value)
    {
        this.startTimestamp = sts;
        this.endTimestamp = ets;
        this.value = value;
    }

    public long getStartTimestamp()
    {
        return startTimestamp;
    }
    public long getEndTimestamp()
    {
        return endTimestamp;
    }
    public double getValue()
    {
        return value;
    }

    public String toString()
    {
        return "IntervalEvent( start: "    + startTimestamp + "   "+    ",end: " + endTimestamp+   "   "+  "AvgValue: "      + getValue()+ ")";
    }
}
