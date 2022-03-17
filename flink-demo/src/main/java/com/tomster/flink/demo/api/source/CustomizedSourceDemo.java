package com.tomster.flink.demo.api.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author meihewang
 * @date 2021/06/01  14:21
 */
public class CustomizedSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new MySource()).setParallelism(1).print();
        env.execute();
    }

    public static class MySource implements SourceFunction<String> {

        boolean cancelled = false;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < 100 & !cancelled; i++) {
                ctx.collect(String.valueOf(i));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

}
