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
public abstract class RawEvent {

    protected long timestamp;
    protected double value;
    protected String key;

    public RawEvent(long ts, double v)
    {
        timestamp = ts;
        value = v;
        key = "dummy";
    }
    public RawEvent(String k, long ts, double v)
    {
        this.key = k;
        timestamp = ts;
        value = v;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public double getValue()
    {
        return value;
    }

    public String getKey(){ return key;}
}
