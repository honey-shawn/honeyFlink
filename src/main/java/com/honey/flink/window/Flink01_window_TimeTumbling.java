package com.honey.flink.window;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.util.ArrayList;


/**
 * 基于时间的滚动窗口
 * 增量聚合计算：sum、aggregate
 * 全量窗口计算：apply、process
 */
public class Flink01_window_TimeTumbling {
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
        // 开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        // 增量聚合计算
//        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowStream.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowStream.aggregate(new MyAggFunc(), new MyWindowFunc());// 增量聚合并能获取到window信息

        // 全量窗口计算，优势：能获取到窗口信息
//        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
//            @Override
//            public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                // 取出迭代器的长度
//                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(input.iterator());
//                // 输出数据
//                out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + key, arrayList.size()));
//            }
//        });
//        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
//            @Override
//            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
//                // 取出迭代器的长度
//                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(elements.iterator());
//                // 输出数据
//                out.collect(new Tuple2<>(new Timestamp(context.window().getStart()) + ":" + key, arrayList.size()));
//            }
//        });

        // 打印
        result.print();

        // 执行
        env.execute();
    }
    public static class MyAggFunc implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }
    // 增量聚合并能获取到window信息
    public static class MyWindowFunc implements WindowFunction<Integer,Tuple2<String,Integer>,String,TimeWindow>{
        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 取出迭代器中的数据
            Integer next = input.iterator().next();
            // 输出数据
            out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + key,next));
        }
    }

}
