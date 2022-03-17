package com.tomster.flink.demo.api.window;

import com.tomster.flink.demo.api.window.aggregateFunction.ClassMeanAgeAggregateFunction;
import com.tomster.flink.demo.entity.Student;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2022/01/05  10:15
 */
public class IncrementalAggregationFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\" + "students.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        DataStream<Student> dataStream = inputDataStream.map(line -> {
            String[] split = line.split(" ");
            return new Student(split[0], Integer.parseInt(split[1]), split[2], Integer.parseInt(split[3]));
        });
        SingleOutputStreamOperator<Double> stream = dataStream.keyBy("classNo")
                .countWindow(6)
                .aggregate(new ClassMeanAgeAggregateFunction());//增量聚合

        stream.print();

        env.execute();
    }


}
