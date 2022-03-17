package com.tomster.flink.demo.api.window.windowFunction;

import com.tomster.flink.demo.entity.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author meihewang
 * @date 2022/01/05  10:42
 */
public class SensorWindowFunction implements WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
        String id = s;
        long windowEnd = window.getEnd();
        int count = IteratorUtils.toList(input.iterator()).size();
        out.collect(new Tuple3<>(id, windowEnd, count));
    }
}
