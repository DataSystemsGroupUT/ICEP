/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ut.ee.icep.generator;

import ut.ee.icep.events.IntervalEvent;

/**
 *
 * @author MKamel
 */
public class Match {

    IntervalEvent e1;
    IntervalEvent e2;
    String op;

    public String toString()

    {
        if (e1 == null){ return "";}
        return "Match( First Match: "    + e1 + "   "+    ",Second Match: " + e2 +   "   "+  "Operator: "      + op + ")";
    }

}
