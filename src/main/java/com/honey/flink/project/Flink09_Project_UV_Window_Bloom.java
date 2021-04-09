package com.honey.flink.project;

import com.honey.flink.bean.UVCount;
import com.honey.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * 基于窗口，实时统计每小时内的网站UV
 * 布隆过滤器
 */
public class Flink09_Project_UV_Window_Bloom {
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

        // 使用布隆过滤器 自定义触发器:来一条计算一条（访问redis一次）
        SingleOutputStreamOperator<UVCount> result = windowedStream
                .trigger(new MyTrigger())
                .process(new UVWindowFunc());


        //打印并执行任务
        result.print();
        env.execute();

    }

    // 触发器,来一条计算一条（访问redis一次）
    public static class MyTrigger extends Trigger<UserBehavior,TimeWindow>{

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UVWindowFunc extends ProcessWindowFunction<UserBehavior,UVCount,String,TimeWindow>{
        // redis链接
        private Jedis jedis;

        // 布隆过滤器
        private MyBloomFilter myBloomFilter;

        // 声明每个窗口总人数的key
        private String hourUvCountKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            // redis初始化
            jedis = new Jedis("hadoop102",6397);
            hourUvCountKey = "HourUv";
            myBloomFilter = new MyBloomFilter((long) (1 << 30));// 1向左移动30位，10亿
        }

        @Override
        public void process(String key, Context context, Iterable<UserBehavior> elements, Collector<UVCount> out) throws Exception {
            // 取出数据
            UserBehavior userBehavior = elements.iterator().next();

            // 提取窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            // 定义当前窗口的BitMap Key
            String bitMapKey = "BitMap_" + windowEnd;

            // 查询当前的UID是否已经存在于当前的bitMap中
            long offset = myBloomFilter.getOffset(userBehavior.getUserId().toString());
            Boolean exist = jedis.getbit(bitMapKey, offset);

            // 根据数据是否存在决定下一步操作
            if(!exist){
                //将对应的offset位置改为1
                jedis.setbit(bitMapKey, offset, true);

                // 累加当前窗口的总和
                jedis.hincrBy(hourUvCountKey, windowEnd, 1);
            }
            // 输出数据
            String hget = jedis.hget(hourUvCountKey, windowEnd);
            out.collect(new UVCount("UV", windowEnd, Integer.parseInt(hget)));
        }
    }

    // 自定义布隆过滤器
    public static class MyBloomFilter {
        // 布隆过滤器容量,最好传入2的整次幂数据
        private long cap;

        public MyBloomFilter(Long cap) {
            this.cap = cap;
        }

        // 传入一个字符串，获取在BitMap中的位置
        public long getOffset(String value){
            long result = 0L;

            for(char c : value.toCharArray()) {
                result += result * 31 + c;
            }

            // 取模
            return result & (cap - 1);
        }
    }

}
