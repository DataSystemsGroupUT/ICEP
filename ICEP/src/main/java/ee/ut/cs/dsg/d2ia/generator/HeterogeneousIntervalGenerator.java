package ee.ut.cs.dsg.d2ia.generator;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Condition;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;



import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class HeterogeneousIntervalGenerator<S extends RawEvent, E extends RawEvent, F extends RawEvent, W extends IntervalEvent> implements Serializable {

    private Class<S> startTypeClass;
    private Class<E> endTypeClass;

    private Class<F> forbiddenTypeClass;
    private Class<W> targetTypeClass;


    private DataStream<S> startStream;
    private DataStream<E> endStream;
    private DataStream<W> targetStream;
    private DataStream<F> forbiddenStream;

    private Condition startStreamCondition;
    private Condition endStreamCondition;

    private Time within;


    public HeterogeneousIntervalGenerator startEventType(Class<S> srctype) {
        this.startTypeClass = srctype;
        return this;
    }

    public HeterogeneousIntervalGenerator endEventType(Class<E> srctype) {
        this.endTypeClass = srctype;
        return this;
    }

    public HeterogeneousIntervalGenerator startSource(DataStream<S> srstream) {
        this.startStream = srstream;
        return this;
    }

    public HeterogeneousIntervalGenerator endSource(DataStream<E> endStream) {
        this.endStream = endStream;
        return this;
    }

    public HeterogeneousIntervalGenerator forbiddenStream(DataStream<F> forbiddenStream) {
        //TODO: Generalize to make a list of forbidden streams
        this.forbiddenStream = forbiddenStream;
        return this;
    }


    public HeterogeneousIntervalGenerator target(DataStream<W> trgtStream) {
        this.targetStream = trgtStream;
        return this;
    }

    public HeterogeneousIntervalGenerator targetType(Class<W> targetTyp) {
        this.targetTypeClass = targetTyp;
        return this;
    }

    public HeterogeneousIntervalGenerator startStreamCondition(AbsoluteCondition cond) {
        this.startStreamCondition = cond;
        return this;
    }

    public HeterogeneousIntervalGenerator endStreamCondition(Condition cond) {
        this.endStreamCondition = cond;
        return this;
    }

    public HeterogeneousIntervalGenerator within(Time t) {
        this.within = t;
        return this;
    }

    private void validate() throws Exception {
        if (startStream == null) {
            throw new Exception("Stream of start events must be defined");
        }

        if (endStream == null) {
            throw new Exception("Stream of end events must be defined");
        }

    }

    public DataStream<W> run() throws Exception {
        validate();
        Pattern<RawEvent, ?> pattern = Pattern.<RawEvent>begin("start").where(new SimpleCondition<RawEvent>() {
            @Override
            public boolean filter(RawEvent s) throws Exception {
                if (startStreamCondition == null)
                    return true;
                else {
                    ScriptEngineManager mgr = new ScriptEngineManager();
                    ScriptEngine engine = mgr.getEngineByName("JavaScript");
                    String conditionString = startStreamCondition.toString();


                    conditionString = conditionString.replace("value", Double.toString(s.getValue()));
                    boolean result = (boolean) engine.eval(conditionString);
                    return result;
                }
            }
        }).subtype(startTypeClass);


        if (forbiddenStream != null) {
            pattern = pattern.notFollowedBy("forbidden").subtype(forbiddenTypeClass);
        }
        pattern = pattern.followedBy("end").where(new RelativeIterativeCondition<>(endStreamCondition, RelativeIterativeCondition.ConditionContainer.Where)).subtype(endTypeClass);

        if (within != null) {
            pattern = pattern.within(within);
        }


        // Partition the streams by key and use their RawEvent representation
        DataStream<RawEvent> startStreamAsRawEvent = startStream.keyBy(new KeySelector<S, String>() {
            @Override
            public String getKey(S s) throws Exception {
                return s.getKey();
            }
        }).map(new MapFunction<S, RawEvent>() {
            @Override
            public RawEvent map(S s) throws Exception {
                return s;
            }
        });

        DataStream<RawEvent> endStreamAsRawEvent = endStream.keyBy(new KeySelector<E, String>() {
            @Override
            public String getKey(E s) throws Exception {
                return s.getKey();
            }
        }).map(new MapFunction<E, RawEvent>() {
            @Override
            public RawEvent map(E s) throws Exception {
                return s;
            }
        });
        DataStream<RawEvent> forbiddenStreamAsRawEvent;
        DataStream<RawEvent> all = startStreamAsRawEvent.union(endStreamAsRawEvent);
        if (forbiddenStream != null) {
            forbiddenStreamAsRawEvent = forbiddenStream.keyBy(new KeySelector<F, String>() {
                @Override
                public String getKey(F s) throws Exception {
                    return s.getKey();
                }
            }).map(new MapFunction<F, RawEvent>() {
                @Override
                public RawEvent map(F f) throws Exception {
                    return f;
                }
            });
            all = all.union(forbiddenStreamAsRawEvent);
        }
        PatternStream<RawEvent> patternStream = CEP.pattern(all, pattern);

        targetStream = patternStream.select(new HeterogeneousIntervalElementsCollector<>(targetTypeClass), TypeInformation.of(targetTypeClass));

        return targetStream;
    }


}
