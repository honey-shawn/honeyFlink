package com.honey.flink.transform;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink15_TransForm_process_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("10.100.217.124", 9990);

        // 2.使用process实现压平
        SingleOutputStreamOperator<String> wordDS = socketTextStream.process(new ProcessFlatMapFunc());

        // 3.使用process实现Map功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.process(new ProcessMapFunc());

        // 4.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        // 5.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        // 6.打印
        sum.print();

        env.execute();

    }

    public static class ProcessFlatMapFunc extends ProcessFunction<String,String>{
        @Override
        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // 运行时上下文，状态编程
            RuntimeContext runtimeContext = getRuntimeContext();

            String[] words = value.split(",");
            for (String word : words) {
                out.collect(word);
            }

            // 定时器
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(12345L);

            // 获取当前处理数据时间
            timerService.currentProcessingTime();
            timerService.currentWatermark();

        }
    }

    public static class ProcessMapFunc extends ProcessFunction<String, Tuple2<String,Integer>>{
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value,1));
        }
    }
}
