package com.honey.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 基于时间的回话窗口
 */
public class Flink03_window_TimeSession {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("10.100.217.124", 9990);
        // 压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDs = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        // 按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDs.keyBy(data -> data.f0);
        // 开窗，回话窗口，回话间隔时间5秒
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        // 聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);
        // 打印
        result.print();

        // 执行
        env.execute();


    }
}
