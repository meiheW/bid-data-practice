package com.tomster.flink.demo.api.sink;

import com.tomster.flink.demo.entity.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author meihewang
 * @date 2021/06/02  20:07
 */
public class RedisSinkDemo {


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

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        mapStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute("redis sink job");
    }


    public static class MyRedisMapper implements RedisMapper<Student> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "student_info");
        }

        @Override
        public String getKeyFromData(Student student) {
            return student.getName();
        }

        @Override
        public String getValueFromData(Student student) {
            return student.toString();
        }
    }

}
