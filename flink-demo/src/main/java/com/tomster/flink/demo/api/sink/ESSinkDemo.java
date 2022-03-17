package com.tomster.flink.demo.api.sink;

import com.tomster.flink.demo.entity.Student;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author meihewang
 * @date 2021/06/02  21:47
 */
public class ESSinkDemo {

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

        // 定义es的连接配置
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<Student>(httpHosts, new MyEsSinkFunction()).build());

        env.execute("es sink job");
    }

    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<Student> {
        @Override
        public void process(Student element, RuntimeContext ctx, RequestIndexer indexer) {
            // 定义写入的数据source
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("name", element.getName());
            dataSource.put("gender", element.getGender());
            dataSource.put("age", String.valueOf(element.getAge()));
            dataSource.put("classNo", String.valueOf(element.getClassNo()));

            // 创建请求，作为向es发起的写入命令(ES7统一type就是_doc，不再允许指定type)
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("student")
                    .source(dataSource);

            // 用index发送请求
            indexer.add(indexRequest);
        }
    }

}
