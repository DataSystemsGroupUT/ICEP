/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ut.ee.Events;

import ut.ee.icep.events.IntervalEvent;

/**
 *
 * @author MKamel
 */

public class TemperatureWarning extends IntervalEvent{
    
    private double averageTemperature;
    private long fts;
    private long ets;
    
  public TemperatureWarning(long sts, long ets, double value){            
          super(sts, ets, value);
    }

        @Override
     public long getStartTimestamp()
    {
        return startTimestamp;
    }

        @Override
    public long getEndTimestamp()
    {
        return endTimestamp;
    }
    public double getValue()
    {
        return value;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureWarning) {
            TemperatureWarning other = (TemperatureWarning) obj;
            return  averageTemperature == other.averageTemperature;
        } else {
            return false;
        }
    }
    @Override
    public int hashCode() {
        return  Double.hashCode(averageTemperature);
    }


   @Override
    public String toString() {
       return "Temp Wairning (Interval      ,  Avg Temp =" + averageTemperature + ", "
              +   "First Time Stampe =" + fts+ "," +     "last  Time Stampe ="  + ets+  ")";
               }
}
              
    
