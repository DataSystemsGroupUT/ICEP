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
public abstract class RawEvent {

private int rackID;


    protected long timestamp;
    protected double value;
    protected String key;

    public RawEvent(int rackid,long ts, double v)
    {
        timestamp = ts;
        value = v;
        this.rackID=rackid;
    }
    public RawEvent(String k, long ts, double v)
    {
        this.key = k;
        timestamp = ts;
        value = v;
    }
    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
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


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RawEvent) {
            RawEvent monitoringEvent = (RawEvent) obj;
            return monitoringEvent.canEquals(this) && rackID == monitoringEvent.rackID;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return rackID;
    }

    public boolean canEquals(Object obj) {
        return obj instanceof RawEvent;
    }
}
