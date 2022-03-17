package com.tomster.flink.demo.api.transform;

import com.tomster.flink.demo.entity.SensorReading;
import com.tomster.flink.demo.entity.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2021/06/02  14:11
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        DataStream<SensorReading> mapStream = dataStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        KeyedStream<SensorReading, String> keyStream = mapStream.keyBy(SensorReading::getSensor);
        DataStream<SensorReading> ageMaxStream = keyStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading curSensor, SensorReading newSensor) throws Exception {
                return new SensorReading(curSensor.getSensor(), newSensor.getTs(),
                        Math.max(curSensor.getTemperature(), newSensor.getTemperature()));
            }
        });
        //DataStream<SensorReading> ageMaxStream = keyStream.maxBy("temperature");
        ageMaxStream.print("temperature");
        env.execute("reduce");
    }
}
