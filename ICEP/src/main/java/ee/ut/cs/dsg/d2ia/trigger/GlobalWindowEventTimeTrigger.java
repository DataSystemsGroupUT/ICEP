package ee.ut.cs.dsg.d2ia.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class GlobalWindowEventTimeTrigger extends Trigger<Object, GlobalWindow> {

    private long lastWatermark=0;
    @Override
    public TriggerResult onElement(Object o, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

        if (lastWatermark >= triggerContext.getCurrentWatermark())
            return TriggerResult.CONTINUE;
        else
        {
            lastWatermark = triggerContext.getCurrentWatermark();
            return  TriggerResult.FIRE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        if (l < triggerContext.getCurrentWatermark())
            return TriggerResult.CONTINUE;
        else
            return TriggerResult.FIRE;
    }

    @Override
    public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

    }
}
