package com.tomster.flink.demo.api.window;

import com.tomster.flink.demo.api.window.windowFunction.SensorWindowFunction;
import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author meihewang
 * @date 2022/01/05  10:39
 */
public class FullWindowFunctions {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\" + "sensor.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        DataStream<SensorReading> mapStream = inputDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = mapStream.keyBy(SensorReading::getSensor)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
                .apply(new SensorWindowFunction());//全窗口函数

        apply.print();

        env.execute();
    }

}
