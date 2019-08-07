package ee.ut.cs.dsg.d2ia.processor;

import ee.ut.cs.dsg.d2ia.condition.*;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class D2IAHomogeneousIntervalProcessorFunction<E extends  RawEvent, I extends IntervalEvent> extends ProcessWindowFunction<E ,I , String, GlobalWindow> {

    private int minOccurs, maxOccurs;
    private Condition condition;
    private ConditionEvaluator<E> conditionEvaluator;
    private Time within;
    private Operand outputValueCalculator;
//    private int count;
//    private double sum;
    //TODO: Let's check working with states later on to handle incomplete windows (frames)
    //private ListStateDescriptor<E> elementsRemainingFromPreviousFiring

    public D2IAHomogeneousIntervalProcessorFunction(int minOccurs, int maxOccurs, Condition cnd, Time within, Operand outputValue)
    {
        this.minOccurs = minOccurs <= 0? Integer.MIN_VALUE: minOccurs;
        this.maxOccurs = maxOccurs <= 0? Integer.MAX_VALUE: maxOccurs;
        this.condition = cnd;
        this.conditionEvaluator = new ConditionEvaluator<>();
        this.within = within==null? Time.milliseconds(0): within;
        this.outputValueCalculator = outputValue== null? Operand.Average: outputValue;
//        count = 0;
//        sum=0;
    }

    private boolean isOccurrencesConditionEffective()
    {
        return !(minOccurs==Integer.MIN_VALUE && maxOccurs==Integer.MAX_VALUE);

    }
    private boolean evaluateOccurrencesCondition(int frameSize)
    {
        if (minOccurs==Integer.MIN_VALUE && maxOccurs == Integer.MAX_VALUE)
            return true;
        else if (minOccurs==Integer.MIN_VALUE && maxOccurs < frameSize)
            return false;
        else if (minOccurs > frameSize && maxOccurs== Integer.MAX_VALUE)
            return false;
        else if (minOccurs > frameSize || maxOccurs < frameSize)
            return false;
//        else if (maxOccurs > frameSize)
//            return false;
        else
            return true;
    }


    private void emitInterval(String s, ArrayList<E> elements, Collector<I> collector)
    {


        double outputValue = 0;
        String rid = elements.get(0).getKey();
        String outValueDescription = outputValueCalculator.toString();
        if (outputValueCalculator == Operand.First) {
            outputValue = elements.get(0).getValue();

        } else if (outputValueCalculator == Operand.Last) {
            outputValue = elements.get(elements.size() - 1).getValue();
        } else if (outputValueCalculator == Operand.Average) {
            for (E e: elements) {
                outputValue += e.getValue();
            }
            outputValue = outputValue / elements.size();
        } else if (outputValueCalculator == Operand.Sum) {
            for (E e : elements) {
                outputValue += e.getValue();
            }
        } else if (outputValueCalculator == Operand.Max ){
            outputValue = elements.get(0).getValue();
            for (E e:elements)
            {
                outputValue = Double.max(outputValue, e.getValue());
            }
        } else if (outputValueCalculator == Operand.Min ) {
            outputValue = elements.get(0).getValue();
            for (E e : elements) {
                outputValue = Double.min(outputValue, e.getValue());
            }
        }
        long start, end;
        start = elements.get(0).getTimestamp();
        end = elements.get(elements.size()-1).getTimestamp();
        I element = (I) new IntervalEvent(start,end, outputValue, outValueDescription, s);
        collector.collect(element);
    }
    @Override
    public void process(String s, Context context, Iterable<E> iterable, Collector<I> collector) throws Exception {
        ArrayList<E> sorted = new ArrayList();

        for (E e: iterable) {
            //Enforce watermark rule
//                    if (e.getTimestamp() <= context.currentWatermark())
            sorted.add(e);

        }
        sorted.sort((o1, o2) -> {
            if (o1.getTimestamp() < o2.getTimestamp()) return -1;
            if (o1.getTimestamp() > o2.getTimestamp()) return 1;
            return 0;
        });

        //let's say we output threshold intervals with condition temperature <= 21
        long start=0, end=0;
     //   double value=0;
        int i =0;
        E event=null;
        E previousEvent=null;
        boolean brokenFromLoop=false;

        // loop over elements and start constructing frames
        // first check the condition
        // then check the time lapse (within)
        // then check the min/max occurrences
        // finally compute the value
        ArrayList<E> currentFrame = new ArrayList<>(sorted.size());
        ArrayList<E> toBeEvicted = new ArrayList<>(sorted.size());
        for (; i < sorted.size();i++)
        {
            boolean conditionPassed=false;
            boolean withinIntervalCheckPassed=true;
            boolean occurrencesPassed=false;
            event = sorted.get(i);


            if (event.getTimestamp() > context.currentWatermark()) {
                brokenFromLoop=true;
                break; // no need to process the rest of the elements but we can emit the current complete window, if any
            }

            if (condition instanceof AbsoluteCondition) {
                conditionPassed =conditionEvaluator.evaluateCondition((AbsoluteCondition) condition, event);


            }
            else if (condition instanceof RelativeCondition)
            {
                conditionPassed = conditionEvaluator.evaluateRelativeCondition((RelativeCondition) condition,currentFrame,event);

            }
            if (previousEvent != null)
            {
                withinIntervalCheckPassed = event.getTimestamp() - previousEvent.getTimestamp() <= this.within.toMilliseconds();
            }

            //

            if (conditionPassed)
            {
                if (withinIntervalCheckPassed) {
                    currentFrame.add(event);
                    if(evaluateOccurrencesCondition(currentFrame.size()))
                        emitInterval(s, currentFrame, collector);
                }
                else
                {
                    toBeEvicted.addAll(currentFrame);
                    currentFrame.clear();
                }

            }
            else // interval is broken based on values
            {
                if(evaluateOccurrencesCondition(currentFrame.size()))
                    emitInterval(s, currentFrame, collector);
                // Either we are eligible to emit or not we have to clear the buffer as future items should belong to another frame
                toBeEvicted.addAll(currentFrame);
                currentFrame.clear();
            }

            //   if (conditionPassed && !withinIntervalCheckPassed & !occurrencesPassed) // we
            // this was here for testing and shall be completely removed
//            if (event.getValue() <= 20.0)
//            {
//                if (start==0) // we start a new interval
//                    start = event.getTimestamp();
//                end = event.getTimestamp();
//                value+= event.getValue();
//            }
//            else
//            {
//                if (start !=0) {
//                    //collector.collect(new Tuple4<>(o, start, end, value));
//                    collector.collect( (I) new IntervalEvent(start, end, value, outputValueCalculator.toString(), s));
//                    start = 0;
//                    end = 0;
//                    value = 0;
//                }
//            }
            previousEvent = sorted.get(i);
        }
//        long keepuntilTs=context.currentWatermark();
//        if (event != null)
//            keepuntilTs = event.getTimestamp();

//        if (start !=0 && !brokenFromLoop) // we processed all elements normally
//            // collector.collect(new Tuple4<>(o,start,end,value));
//            collector.collect( (I) new IntervalEvent(start, end, value, outputValueCalculator.toString(), s));
//        else if (start != 0 && brokenFromLoop && event.getValue() > 20.0) // we received an item with future timestamp (greater than watermark) but its value is breaking the theta condition
//            collector.collect( (I) new IntervalEvent(start, end, value, outputValueCalculator.toString(), s));
//        else if (start!=0 && brokenFromLoop && event.getValue() <= 20.0)
//            keepuntilTs = start;
        // ugly but necessary, to clean here not in the evictor, actually, I will drop the evictor
        for (Iterator<E> iterator = iterable.iterator(); iterator.hasNext();){
            E v =  iterator.next();
            if (toBeEvicted.contains(v))
            {
                iterator.remove();
            }

        }

    }
}
