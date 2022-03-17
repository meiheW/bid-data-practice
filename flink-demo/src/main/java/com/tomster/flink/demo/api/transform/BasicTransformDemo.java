package com.tomster.flink.demo.api.transform;

import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author meihewang
 * @date 2021/06/02  10:30
 */
public class BasicTransformDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        DataStream<SensorReading> mapStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        mapStream.print("map");

        DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (int i = 0; i < split.length; i++) {
                    out.collect(split[i]);
                }
            }
        });
        flatMapStream.print("flatMap");

        //filter构造函数可以带参数
        DataStream<String> filterStream = dataStream.filter(new SensorFilter("sensor"));
        filterStream.print("filter");

        env.execute();
    }


    public static class SensorFilter implements FilterFunction<String> {

        private String sensor;

        public SensorFilter(String sensor) {
            this.sensor = sensor;
        }

        @Override
        public boolean filter(String value) throws Exception {
            return value.contains(sensor) & value.contains("_2");
        }
    }

}
