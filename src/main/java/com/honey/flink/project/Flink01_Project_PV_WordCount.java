package com.honey.flink.project;

import com.honey.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 总浏览量（PV）的统计
 */
public class Flink01_Project_PV_WordCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文件数据
        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.txt");
        // 转换成javBean并过滤出PV的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4])
                );
                if ("pv".equals(userBehavior.getBehavior())) { // 过滤出pv行为
                    out.collect(new Tuple2<>("PV", 1));
                }
            }
        });
        // 指定keyBy分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = pv.keyBy(data -> data.f0);

        // 计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 打印输出
        result.print();

        env.execute();
    }
}
