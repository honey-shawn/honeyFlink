package com.honey.flink.wordcount;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 无界流
 */
public class Flink02_WordCount_unBounded {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度
        // 2. 读取文件
        DataStreamSource<String> socketTextStream = env.socketTextStream("10.100.217.123", 9990);

        // 3.将每行数据压平并转换成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new Flink02_WordCount_Bounded.LineToTupleFlatMapFunc());

        // 4.分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 5.聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6.打印结果
        result.print();

        env.execute();

        // 3. 转换数据格式
//        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = socketTextStream
//                .flatMap((String line, Collector<String> words) -> {
//                    Arrays.stream(line.split(" ")).forEach(words::collect);
//                })
//                .returns(Types.STRING)
//                .map(word -> Tuple2.of(word, 1L))
//                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
//        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
//                .keyBy(t -> t.f0);
        // 5. 求和
//        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
//                .sum(1);
        // 6. 打印
//        result.print();
        // 7. 执行
//        env.execute();

    }
}
