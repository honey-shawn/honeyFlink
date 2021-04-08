package com.honey.flink.project;

import com.honey.flink.bean.UVCount;
import com.honey.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * 基于窗口，实时统计每小时内的网站UV
 */
public class Flink07_Project_UV_Window {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.txt");

        // 转换成javBean，根据行为过滤数据，并提取时间戳生产WaterMark
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });
        SingleOutputStreamOperator<UserBehavior> userBehaviorDs = readTextFile.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(Long.parseLong(split[0])
                    , Long.parseLong(split[1])
                    , Integer.parseInt(split[2])
                    , split[3]
                    , Long.parseLong(split[4])
            );
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照行为分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDs.keyBy(UserBehavior::getBehavior);

        // 开窗
        WindowedStream<UserBehavior, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        // 使用HashSet方式
        SingleOutputStreamOperator<UVCount> result = windowedStream.process(new UserVisitorProcessWindowFunc());

        //打印并执行任务
        result.print();
        env.execute();

    }
    public static class UserVisitorProcessWindowFunc extends ProcessWindowFunction<UserBehavior, UVCount,String,TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UVCount> out) throws Exception {
            // 创建HashSet用于去重
            HashSet<Long> uids = new HashSet<>();

            // 取出窗口中的所有数据
            Iterator<UserBehavior> iterator = elements.iterator();

            // 遍历迭代器，将数据中的UID放入HashSet，去重
            while (iterator.hasNext()){
                uids.add(iterator.next().getUserId());
            }

            // 输出数据
            out.collect(new UVCount("UV"
                    ,new Timestamp(context.window().getEnd()).toString(),
                    uids.size()));

        }
    }
}
