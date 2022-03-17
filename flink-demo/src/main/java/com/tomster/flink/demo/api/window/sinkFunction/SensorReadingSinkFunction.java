package com.tomster.flink.demo.api.window.sinkFunction;

import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author meihewang
 * @date 2022/01/05  10:01
 */
public class SensorReadingSinkFunction implements SinkFunction<SensorReading> {

    @Override
    public void invoke(SensorReading sr, Context context) throws Exception {
        System.out.println(sr.getSensor() + "---" + sr.getTs() + "---" + sr.getTemperature());
    }
}
