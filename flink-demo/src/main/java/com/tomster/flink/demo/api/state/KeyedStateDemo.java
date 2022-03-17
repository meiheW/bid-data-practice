package com.tomster.flink.demo.api.state;

import com.tomster.flink.demo.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2022/01/17  9:19
 */
public class KeyedStateDemo {

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
        //使用自定义map方法，里面使用我们自定义的Keyed State
        DataStream<Integer> resultStream = mapStream
                .keyBy(SensorReading::getSensor)
                .map(new MyMapper());

        resultStream.print("result");
        env.execute();
    }

    public static class MyMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("my-int", Integer.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = valueState.value();
            // 第一次获取是null，需要判断
            count = count == null ? 0 : count;
            ++count;
            valueState.update(count);
            return count;
        }
    }

}
