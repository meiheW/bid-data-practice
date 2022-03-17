package com.tomster.flink.demo.api.source;

import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;

/**
 * @author meihewang
 * @date 2021/06/01  11:19
 */
public class CollectionSourceDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStream<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        dataStream.print("SENSOR");
        intStream.print("INT");

        intStream.addSink(new SinkFunction<Integer>() {
            @Override
            public void invoke(Integer value, Context context) throws Exception {
                System.out.println("cur val is:" + value);
            }
        }).uid("INT SINK").name("INT SINK");
        env.execute("collection source job");
    }

}
