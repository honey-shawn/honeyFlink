package com.honey.flink.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 有界流
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置执行模式
        env.setParallelism(1); // 设置并行度
        // 2. 读取文件
        DataStreamSource<String> lineDSS = env.readTextFile("input/word.txt");
        // 3.压平并将单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDs = lineDSS.flatMap(new LineToTupleFlatMapFunc());
        // 4.分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        // 5.按照key做聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6.打印结果
        result.print();
        // 7. 执行
        env.execute("Flink02_WordCount_Bounded");

    }

    public static class LineToTupleFlatMapFunc implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按照空格切分数据
            String[] words = value.split(" ");

            //遍历写出
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));

            }
        }
    }
}
