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

    private int rackID;

    protected long startTimestamp;
    protected long endTimestamp;
    protected double value;

    protected String outvalue;





    public IntervalEvent(int rackID,long sts, long ets, double value, String outv)
    {

        this.startTimestamp = sts;
        this.endTimestamp = ets;
        this.value = value;
        this.rackID = rackID;
        this.outvalue = outv;

    }

    public int getRackID() {
        return rackID;
    }

//    public void setRackID(int rackID) {
//        this.rackID = rackID;
//    }



    public long getStartTimestamp()
    {
        return startTimestamp;
    }
    public long getEndTimestamp()
    {
        return endTimestamp;
    }
    public double getAvgValue()
    {
        return value;

    }

    public String getoutvalue()
    {
//        System.out.println(outvalue);

        return outvalue;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntervalEvent ) {
            IntervalEvent other = (IntervalEvent) obj;

            return rackID == other.rackID && value == other.value;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * rackID + Double.hashCode(value);
    }


//    public String toString()
//    {
//
//        return "IntervalEvent( " + getRackID() + ", start: "    + startTimestamp + "   "+    ",end: " + endTimestamp+   "   "+  "AvgValue: "      + getAvgValue()+ " " + getoutvalue() +")";
//    }
}
