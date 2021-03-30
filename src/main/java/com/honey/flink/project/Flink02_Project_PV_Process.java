package com.honey.flink.project;

import com.honey.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 总浏览量（PV）的统计
 */
public class Flink02_Project_PV_Process {
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
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDs.keyBy(data -> "PV");

        // 计算总和
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            Integer count = 0;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                count++;
                out.collect(count);
            }
        });

        // 打印输出
        result.print();

        env.execute();
    }
}
