package com.tomster.flink.demo.api.window;

import com.tomster.flink.demo.api.window.sinkFunction.SensorReadingSinkFunction;
import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2022/01/04  9:29
 */
public class TimeWindowDemo {

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
        }).keyBy(SensorReading::getSensor)
                .countWindow(2)//滚动计数窗口
                .minBy("temperature");//按字段取最小值
        mapStream.addSink(new SensorReadingSinkFunction());

        env.execute("time window job");
    }

}
