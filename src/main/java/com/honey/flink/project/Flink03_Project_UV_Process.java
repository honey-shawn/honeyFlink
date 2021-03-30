package com.honey.flink.project;

import com.honey.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * 独立访客数（UV）的统计
 */
public class Flink03_Project_UV_Process {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文件数据
        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.txt");
        // 转换成javBean并过滤出PV的数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDs = readTextFile.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4])
                );
                if ("pv".equals(userBehavior.getBehavior())) { // 过滤出pv行为
                    out.collect(userBehavior);
                }
            }
        });
        // 指定keyBy分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDs.keyBy(data -> "UV");

        // 使用process方式计算UV总和（注意userid的去重）
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {

            private HashSet<Long> uids = new HashSet<>();
            private Integer count = 0;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                if (!uids.contains(value.getUserId())){ // 新来的uuid是否包含在集合中
                    uids.add(value.getUserId());
                    count++;
                    out.collect(count);
                }
            }
        });

        // 打印输出
        result.print();

        env.execute();
    }
}
