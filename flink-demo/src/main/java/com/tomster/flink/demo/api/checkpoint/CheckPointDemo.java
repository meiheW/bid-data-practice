package com.tomster.flink.demo.api.checkpoint;

import com.tomster.flink.demo.entity.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2022/03/10  10:22
 */
public class CheckPointDemo {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend(""));
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(50000L);

        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\students.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        DataStream<Student> mapStream = dataStream.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] split = value.split(" ");
                return new Student(split[0], Integer.parseInt(split[1]), split[2], Integer.parseInt(split[3]));
            }
        });
    }


}
