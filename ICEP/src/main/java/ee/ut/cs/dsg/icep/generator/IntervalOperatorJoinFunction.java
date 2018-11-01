package ee.ut.cs.dsg.icep.generator;

import ee.ut.cs.dsg.icep.event.IntervalEvent;
import org.apache.flink.api.common.functions.JoinFunction;

import java.util.List;

public class IntervalOperatorJoinFunction<L extends IntervalEvent, R extends IntervalEvent> implements JoinFunction<L,R, Match> {
    private List<Match.MatchType> filterForMatchTypes;

    public IntervalOperatorJoinFunction(List<Match.MatchType> keepThose) {
        filterForMatchTypes = keepThose;
    }
    @Override
    public Match join(L l, R r)  {
        Match m = Match.getMatch(l,r);
        if (filterForMatchTypes.size() !=0)
        {
            if (filterForMatchTypes.contains(m.getMatchType()))
                return m;
            return null;
        }
        return m;
    }

}
