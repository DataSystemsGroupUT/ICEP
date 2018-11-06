/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.d2ia.generator;


import ee.ut.cs.dsg.d2ia.event.IntervalEvent;

/**
 * @author MKamel
 */
public class Match {

    public enum MatchType
    {
        Before,
        After,
        Overlaps,
        During,
        Contains,
        Meets,
        MetBy,
        Starts,
        StartedBy,
        Finishes,
        FinishedBy,
        Equals;

        @Override
        public String toString()
        {
            switch(this){
                case Before: return " < ";
                case After: return " > ";
                case Overlaps: return " o ";
                case During: return " d ";
                case Contains: return " di ";
                case Meets: return " m ";
                case MetBy: return " mi ";
                case Starts: return " s ";
                case StartedBy: return  " si ";
                case Finishes: return " f ";
                case FinishedBy: return " fi ";
                case Equals: return  " = ";
                default: throw new IllegalArgumentException();
            }
        }
    }
    private IntervalEvent lhsIntervalEvent;
    private IntervalEvent rhsIntervalEvent;
    private MatchType matchType;

    public IntervalEvent getLeftInterval(){return  lhsIntervalEvent;}
    public IntervalEvent getRightInterval() {return rhsIntervalEvent;}
    public MatchType getMatchType(){return matchType;}
    private Match(IntervalEvent lhs, IntervalEvent rhs, MatchType matchType)
    {
        this.lhsIntervalEvent = lhs;
        this.rhsIntervalEvent = rhs;
        this.matchType = matchType;
    }
    @Override
    public String toString() {
        if (lhsIntervalEvent != null && rhsIntervalEvent != null && matchType != null)
            return lhsIntervalEvent.toString()+ matchType.toString()+rhsIntervalEvent.toString();
        else
            return "One or more attributes are undefined";
    }
    public static MatchType getMatchType(IntervalEvent e1, IntervalEvent e2)
    {
        if (e1.getEndTimestamp() < e2.getStartTimestamp())
            return MatchType.Before;
        else if (e1.getStartTimestamp() > e2.getEndTimestamp())
            return MatchType.After;
        else if (e1.getEndTimestamp() == e2.getStartTimestamp() && e1.getStartTimestamp() < e2.getStartTimestamp() && e1.getEndTimestamp() != e2.getEndTimestamp())
            return MatchType.Meets;
        else if (e1.getStartTimestamp() == e2.getEndTimestamp() && e1.getStartTimestamp() > e2.getStartTimestamp() && e1.getEndTimestamp() != e2.getEndTimestamp())
            return MatchType.MetBy;
        else if (e1.getStartTimestamp() < e2.getStartTimestamp() && e1.getEndTimestamp() > e2.getStartTimestamp() && e1.getEndTimestamp() < e2.getEndTimestamp())
            return MatchType.Overlaps;
        else if (e1.getStartTimestamp() > e2.getStartTimestamp() && e1.getStartTimestamp() < e2.getEndTimestamp() && e1.getEndTimestamp() > e2.getEndTimestamp())
            return MatchType.Overlaps;
        else if (e1.getStartTimestamp() > e2.getStartTimestamp() && e1.getStartTimestamp() < e2.getEndTimestamp() && e1.getEndTimestamp() < e2.getEndTimestamp() )
            return MatchType.During;
        else if (e1.getStartTimestamp() == e2.getStartTimestamp() && e1.getEndTimestamp() < e2.getEndTimestamp())
            return MatchType.Starts;
        else if (e1.getStartTimestamp() == e2.getStartTimestamp() && e1.getEndTimestamp() > e2.getEndTimestamp())
            return MatchType.StartedBy;
        else if (e1.getStartTimestamp() > e2.getStartTimestamp() && e1.getEndTimestamp() < e2.getEndTimestamp())
            return MatchType.During;
        else if (e1.getStartTimestamp() < e2.getStartTimestamp() && e1.getEndTimestamp() > e2.getEndTimestamp())
            return MatchType.Contains;
        else if (e1.getStartTimestamp() > e2.getStartTimestamp() && e1.getEndTimestamp() == e2.getEndTimestamp())
            return MatchType.Finishes;
        else if (e1.getStartTimestamp() < e2.getStartTimestamp() && e1.getEndTimestamp() == e2.getEndTimestamp())
            return MatchType.FinishedBy;
        else
            return MatchType.Equals;
    }
    public static Match getMatch(IntervalEvent e1, IntervalEvent e2)
    {
        return new Match(e1, e2, getMatchType(e1,e2));
    }
    @Override
    public boolean equals(Object other)
    {
        if (! (other instanceof  Match))
            return  false;
        Match otherMatch = (Match) other;
        return this.lhsIntervalEvent.equals(otherMatch.getLeftInterval())
                && this.rhsIntervalEvent.equals(otherMatch.getRightInterval())
                && this.matchType == ((Match) other).getMatchType();

    }

}
