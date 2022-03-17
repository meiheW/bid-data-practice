package com.tomster.flink.demo.api.window;

import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author meihewang
 * @date 2022/01/06  16:41
 */
public class EventTimeWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用系统时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //Watermark  ???
        env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(1);

        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\" + "sensor-event.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        SingleOutputStreamOperator<SensorReading> minTempStream = inputDataStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        }).keyBy("sensor")
                .timeWindow(Time.seconds(5))
                .minBy("temperature");

        minTempStream.print("minTemp");

        env.execute();
    }

}
