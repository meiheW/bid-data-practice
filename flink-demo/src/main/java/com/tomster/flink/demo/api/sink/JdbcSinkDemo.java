package com.tomster.flink.demo.api.sink;

import com.tomster.flink.demo.entity.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author meihewang
 * @date 2021/06/02  21:58
 */
public class JdbcSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\students.txt";
        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile(inputPath);
        // 转换成SensorReading类型
        DataStream<Student> dataStream = inputStream.map(line -> {
            String[] split = line.split(" ");
            return new Student(split[0], Integer.parseInt(split[1]), split[2], Integer.parseInt(split[3]));
        });
        dataStream.addSink(new MyJdbcSink());
        env.execute("jdbc sink job");
    }


    public static class MyJdbcSink extends RichSinkFunction<Student> {
        // 声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false", "root", "root");
            insertStmt = connection.prepareStatement("insert into student (`name`,`age`, `gender`, `classNo`) values (?, ?, ?, ?)");
            //updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        // 每来一条数据，调用连接，执行sql
        @Override
        public void invoke(Student value, Context context) throws Exception {
            // 直接执行更新语句，如果没有更新那么就插入
            /*updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();*/
            //if (updateStmt.getUpdateCount() == 0) {
            insertStmt.setString(1, value.getName());
            insertStmt.setInt(2, value.getAge());
            insertStmt.setString(3, value.getGender());
            insertStmt.setInt(4, value.getClassNo());
            insertStmt.execute();
            //}
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            //updateStmt.close();
            connection.close();
        }
    }
}
