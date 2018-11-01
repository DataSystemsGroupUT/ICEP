/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.icep.event;

/**
 *
 * @author MKamel
 */

public class IntervalEvent {

    protected long startTimestamp;
    protected long endTimestamp;
    protected double value;
    protected String valueDescriptor;

    protected String key;
    public IntervalEvent(long sts, long ets, double value, String valueDescriptor, String k)
    {
        this.startTimestamp = sts;
        this.endTimestamp = ets;
        this.value = value;
        this.valueDescriptor = valueDescriptor;
        this.key=k;
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

    public String getValueDescriptor(){return valueDescriptor;}

    public String getKey(){return key;}

    @Override
    public String toString()
    {
        return "IntervalEvent( start: "    + startTimestamp + "   "+    ",end: " + endTimestamp+   "   "+  "Value: "      + getValue()+
                " Value Description:"+ valueDescriptor+")";
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof  IntervalEvent))
            return false;
        IntervalEvent otherInterval = (IntervalEvent) other;
        return this.startTimestamp == otherInterval.getStartTimestamp() && this.endTimestamp == otherInterval.getEndTimestamp()
                && this.value == otherInterval.getValue() && this.valueDescriptor.equals(otherInterval.getValueDescriptor())
                && this.key.equals(otherInterval.getKey());
    }

}
