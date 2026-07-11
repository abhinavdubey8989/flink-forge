package com.flink_forge.windowed_aggregation.aggregate.event_activity;

import com.flink_forge.windowed_aggregation.dto.internal.EventSummary;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;


public class EventSummaryWindowFunction extends
        ProcessWindowFunction<
                Long,
                EventSummary,
                String,
                TimeWindow> {


    @Override
    public void process(
            String eventName,
            Context context,
            Iterable<Long> elements,
            Collector<EventSummary> out) {

        Long count = elements.iterator().next();

        out.collect(
                new EventSummary(
                        eventName,
                        count));
    }
}