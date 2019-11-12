package ee.ut.cs.dsg.d2ia.processor;

import ee.ut.cs.dsg.d2ia.condition.*;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class D2IAHomogeneousIntervalProcessorFunction<E extends RawEvent, I extends IntervalEvent> extends ProcessWindowFunction<E, I, String, TimeWindow> {

    private Class<I> out;
    private Class<E> in;
    private int minOccurs, maxOccurs;
    private Condition condition;
    private ConditionEvaluator<E> conditionEvaluator;
    private long within;
    private Operand outputValueCalculator;
    private boolean generateMaximalInterval = false;
    private boolean itemAdded = false;
    private boolean endOfStream = false;
    private ListStateDescriptor<E> windowState;
//    private int count;
//    private double sum;
    //TODO: Let's check working with states later on to handle incomplete windows (frames)
    //private ListStateDescriptor<E> elementsRemainingFromPreviousFiring

    public D2IAHomogeneousIntervalProcessorFunction(int minOccurs, int maxOccurs, Condition cnd, Time within, Operand outputValue) {
        this.minOccurs = minOccurs <= 0 ? Integer.MIN_VALUE : minOccurs;
        this.maxOccurs = maxOccurs <= 0 ? Integer.MAX_VALUE : maxOccurs;
        this.condition = cnd;
        this.conditionEvaluator = new ConditionEvaluator<>();
        this.within = within == null ? 0 : within.toMilliseconds();
        this.outputValueCalculator = outputValue == null ? Operand.Average : outputValue;

    }

    public D2IAHomogeneousIntervalProcessorFunction(int minOccurs, int maxOccurs, Condition cnd, Time within, boolean generateMaximalInterval, Operand outputValue) {

        this(minOccurs, maxOccurs, cnd, within, outputValue);
        this.generateMaximalInterval = generateMaximalInterval;
    }

    public D2IAHomogeneousIntervalProcessorFunction(int minOccurs, int maxOccurs, Condition cnd, Time within, boolean generateMaximalInterval, Operand outputValue, Class<I> out) {
        this(minOccurs, maxOccurs, cnd, within, generateMaximalInterval, outputValue);
        this.out = out;
    }

    public D2IAHomogeneousIntervalProcessorFunction(int minOccurs, int maxOccurs, Condition cnd, Time within, boolean generateMaximalInterval, Operand outputValue, Class<E> in, Class<I> out) {
        this(minOccurs, maxOccurs, cnd, within, generateMaximalInterval, outputValue, out);
        this.in = in;
        windowState = new ListStateDescriptor<>("ElementsOfIncompleteIntervals", TypeInformation.of(in));
    }

//    private boolean isOccurrencesConditionEffective()
//    {
//        return !(minOccurs==Integer.MIN_VALUE && maxOccurs==Integer.MAX_VALUE);
//
//    }
    private boolean evaluateOccurrencesCondition(int frameSize) {


        if (minOccurs == 1 && maxOccurs == Integer.MAX_VALUE) // both upper and lower bounds set
            return true;
        else if (minOccurs == 1 && maxOccurs < frameSize)
            return false;
        else if (minOccurs > frameSize && maxOccurs == Integer.MAX_VALUE)
            return false;
        else return minOccurs <= frameSize && maxOccurs >= frameSize;


//        if (minOccurs == Integer.MIN_VALUE && maxOccurs == Integer.MAX_VALUE)
//            return true;
//        else if (minOccurs == Integer.MIN_VALUE && maxOccurs < frameSize)
//            return false;
//        else if (minOccurs > frameSize && maxOccurs == Integer.MAX_VALUE)
//            return false;
//        else return minOccurs <= frameSize && maxOccurs >= frameSize;
    }


    private void emitInterval(String s, ArrayList<E> elements, Collector<I> collector) throws Exception {

//        if (endOfStream)
//            System.out.println("Flushing incomplete interval for key: "+s + " due to end of stream signal");
        if (elements.size() == 0)
            return;
        if (!itemAdded && !generateMaximalInterval && !endOfStream)
            return;

//        if (endOfStream)
//            System.out.println("Flushing incomplete interval for key: "+s + " due to end of stream signal");
        double outputValue = 0;
        String rid = elements.get(0).getKey();
        String outValueDescription = outputValueCalculator.toString();
        if (outputValueCalculator == Operand.First) {
            outputValue = elements.get(0).getValue();

        } else if (outputValueCalculator == Operand.Last) {
            outputValue = elements.get(elements.size() - 1).getValue();
        } else if (outputValueCalculator == Operand.Average) {
            for (E e : elements) {
                outputValue += e.getValue();
            }
            outputValue = outputValue / elements.size();
        } else if (outputValueCalculator == Operand.Sum) {
            for (E e : elements) {
                outputValue += e.getValue();
            }
        } else if (outputValueCalculator == Operand.Max) {
            outputValue = elements.get(0).getValue();
            for (E e : elements) {
                outputValue = Double.max(outputValue, e.getValue());
            }
        } else if (outputValueCalculator == Operand.Min) {
            outputValue = elements.get(0).getValue();
            for (E e : elements) {
                outputValue = Double.min(outputValue, e.getValue());
            }
        }
        long start, end;
        start = elements.get(0).getTimestamp();
        end = elements.get(elements.size() - 1).getTimestamp();
        I element;
        if (out != null)
            element = out.getDeclaredConstructor(long.class, long.class, double.class, String.class, String.class).newInstance(start, end, outputValue, outValueDescription, s);
        else
            element = (I) new IntervalEvent(start, end, outputValue, outValueDescription, s);
        collector.collect(element);
    }

    @Override
    public void process(String s, Context context, Iterable<E> iterable, Collector<I> collector) throws Exception {

    //    System.out.println("Processing elemtnts with key: "+s +" for window:"+ context.window().toString());

        ListState<E> elementsFromPreviousFire = context.globalState().getListState(windowState);
        ArrayList<E> sorted = new ArrayList();

        for (E e : iterable) {
            //Enforce watermark rule
//                    if (e.getTimestamp() <= context.currentWatermark())
            sorted.add(e);

        }

        for (E e : elementsFromPreviousFire.get()) {
            //Enforce watermark rule
//                    if (e.getTimestamp() <= context.currentWatermark())
            sorted.add(e);

        }

        elementsFromPreviousFire.clear();

        sorted.sort(Comparator.comparingLong(RawEvent::getTimestamp));

        //let's say we output threshold intervals with condition temperature <= 21

        int i = 0;
        E event;
        E previousEvent = null;
//        boolean brokenFromLoop=false;

        // loop over elements and start constructing frames
        // first check the condition
        // then check the time lapse (within)
        // then check the min/max occurrences
        // finally compute the value
        ArrayList<E> currentFrame = new ArrayList<>(sorted.size());
        //ArrayList<E> toBeEvicted = new ArrayList<>(sorted.size());
        for (; i < sorted.size(); i++) {
            boolean conditionPassed = false;
            boolean withinIntervalCheckPassed = true;

            event = sorted.get(i);
            itemAdded = false;

//            if (event.getTimestamp() > context.currentWatermark()) {
////                brokenFromLoop=true;
//                break; // no need to process the rest of the elements but we can emit the current complete window, if any
//            }

            if (condition instanceof AbsoluteCondition) {
                conditionPassed = conditionEvaluator.evaluateCondition((AbsoluteCondition) condition, event);


            } else if (condition instanceof RelativeCondition) {
                if (currentFrame.size() == 0) // we have to evaluate just the start condition
                {
                    conditionPassed = conditionEvaluator.evaluateCondition(((RelativeCondition) condition).getStartCondition(), event);
                } else
                    conditionPassed = conditionEvaluator.evaluateRelativeCondition((RelativeCondition) condition, currentFrame, event);

            }
            if (previousEvent != null) {
                withinIntervalCheckPassed = this.within == 0 || event.getTimestamp() - previousEvent.getTimestamp() <= this.within;
            }

            //

            if (conditionPassed) {
                if (withinIntervalCheckPassed) {
                    currentFrame.add(event);
                    itemAdded = true;
                    if (evaluateOccurrencesCondition(currentFrame.size()) && !generateMaximalInterval)
                        emitInterval(s, currentFrame, collector);
                } else {
                    // we can emit in case we satisfy the occurrences condition
                    if (evaluateOccurrencesCondition(currentFrame.size()))
                        emitInterval(s, currentFrame, collector);
                    //                   toBeEvicted.addAll(currentFrame);
                    currentFrame.clear();
                }

            } else  // interval is broken based on values
            {
                //There might be a case when the same interval is emitted twice
                // If this was a relative condition and it was not passed we have to check again for the
                if (evaluateOccurrencesCondition(currentFrame.size()))
                    emitInterval(s, currentFrame, collector);

                currentFrame.clear();

                if (condition instanceof RelativeCondition) // we have to evaluate the start condition
                {
                    conditionPassed = conditionEvaluator.evaluateCondition(((RelativeCondition) condition).getStartCondition(), event);
                    if (conditionPassed)
                        currentFrame.add(event);

                }

            }


            previousEvent = sorted.get(i);
        }


//        if (context.currentWatermark() == Long.MAX_VALUE) // this is the end of the stream
//        {
//          //  System.out.println("End of the stream");
//            endOfStream = true;
//            if (evaluateOccurrencesCondition(currentFrame.size()))
//                emitInterval(s, currentFrame, collector);
//        }
//        else

        if (currentFrame.size() > 0)
            elementsFromPreviousFire.addAll(currentFrame);

    }
}
