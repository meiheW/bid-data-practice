package com.tomster.flink.demo.api.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meihewang
 * @date 2021/06/01  13:45
 */
public class TextFileSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\tomster\\big-data-practice\\flink-demo\\src\\main\\resources\\wordcount.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        inputDataStream.print();
        env.execute("text source job");
    }

}
