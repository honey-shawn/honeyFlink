package com.honey.flink.project;

import com.honey.flink.bean.PVCount;
import com.honey.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

/**
 * 基于窗口，实时统计每小时内的网站PV
 * 双重聚合处理数据倾斜
 */
public class Flink08_Project_PV_Window_pre {
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

        // 将数据转换为元组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = userBehaviorDs.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("PV" + new Random().nextInt(8), 1);
            }
        }).keyBy(data -> data.f0);

        // 开窗并计算,第一层聚合
        SingleOutputStreamOperator<PVCount> aggResult = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PVAggFunc(), new PVWindowFunc());

        // 按照窗口信息，重新分组，做第二次聚合
        KeyedStream<PVCount, String> pvCountStringKeyedStream = aggResult.keyBy(data -> data.getTime());

        // 累加结果
        SingleOutputStreamOperator<PVCount> result = pvCountStringKeyedStream.process(new PVProcessFunc());

        // 打印结果
        result.print();

        // 执行
        env.execute();

    }
    
    public static class PVAggFunc implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer>{

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

    public static class PVWindowFunc implements WindowFunction<Integer, PVCount, String , TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<PVCount> out) throws Exception {

            // 提取窗口时间
            String timestamp = new Timestamp(window.getStart()).toString();

            // 获取累积结果
            Integer count = input.iterator().next();

            // 输出结果
            out.collect(new PVCount("PV", timestamp, count));
        }
    }

    public static class PVProcessFunc extends KeyedProcessFunction<String, PVCount, PVCount> {
        // 定义状态
        private ListState<PVCount> listState;

        // 初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<PVCount>("list-state",PVCount.class));
        }

        @Override
        public void processElement(PVCount value, Context ctx, Collector<PVCount> out) throws Exception {
            // 将数据放入状态
            listState.add(value);
            // 注册定时器
            String time = value.getTime();
            long ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
            ctx.timerService().registerEventTimeTimer(ts + 1); // 定时1毫秒
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PVCount> out) throws Exception {
            // 取出状态中的数据
            Iterable<PVCount> pvCounts = listState.get();

            // 遍历累加数据,累加8个并行度里面的数据
            Integer count = 0;
            Iterator<PVCount> iterator = pvCounts.iterator();
            while (iterator.hasNext()){
                PVCount next = iterator.next();
                count += next.getCount();
            }

            // 输出数据,
            out.collect(new PVCount("PV",new Timestamp(timestamp - 1).toString(), count));

            // 清空状态
            listState.clear();
        }
    }
}
