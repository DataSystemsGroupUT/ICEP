/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ut.ee.Events;

import ut.ee.icep.events.IntervalEvent;
import ut.ee.icep.generator.IntervalGenerator;

/**
 *
 * @author MKamel
 */

public class TemperatureWarning extends IntervalEvent{

    private int rackID;
    private String outv;
    IntervalGenerator.Operator op;


  public TemperatureWarning(int rid,long sts, long ets, double value, String outv){
          super(rid,sts, ets, value, outv);

    }



    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureWarning) {
            TemperatureWarning other = (TemperatureWarning) obj;
            return  rackID == other.rackID &&  getAvgValue() == other.getAvgValue();
        } else {
            return false;
        }
    }
    @Override
    public int hashCode() {
        return 41 * rackID +  Double.hashCode(getAvgValue());
    }


   @Override
    public String toString() {

       return "IntervalEventW( K_ID "  + getRackID() + ", " +"start: "    + getStartTimestamp() + "   "+    ",end: " + getEndTimestamp() +   "   "+  getoutvalue()+"_Value: "      + getAvgValue()+ ")";

               }
}
              
    
