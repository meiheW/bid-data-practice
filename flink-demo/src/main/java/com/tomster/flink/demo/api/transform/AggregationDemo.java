package com.tomster.flink.demo.api.transform;

import com.tomster.flink.demo.entity.SensorReading;
import com.tomster.flink.demo.entity.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2021/06/02  13:44
 */
public class AggregationDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\students.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        DataStream<Student> mapStream = dataStream.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] split = value.split(" ");
                return new Student(split[0], Integer.parseInt(split[1]), split[2], Integer.parseInt(split[3]));
            }
        });
        KeyedStream<Student, Integer> keyStream = mapStream.keyBy(Student::getClassNo);
        DataStream<Student> ageMaxStream = keyStream.maxBy("age");
        ageMaxStream.print("student");

        env.execute("aggregation");
    }

}
