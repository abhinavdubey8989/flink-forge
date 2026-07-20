package com.flink_forge.windowed_aggregation.pipeline;

import com.flink_forge.windowed_aggregation.aggregate.event_activity.EventCountAggregationFunction;
import com.flink_forge.windowed_aggregation.aggregate.event_activity.EventSummaryWindowFunction;
import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.windowed_aggregation.dto.internal.EventSummary;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

public class EventAggregationPipeline {

    private EventAggregationPipeline() {
    }

    public static DataStream<EventSummary> build(
            DataStream<UserActivity> events) {
        return events
                .keyBy(event -> event.getEventType().name()) // event-name, enum -> string
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(
                        new EventCountAggregationFunction(),
                        new EventSummaryWindowFunction());
    }
}
