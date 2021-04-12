package com.honey.flink.project;

import com.honey.flink.bean.AdsClickCount;
import com.honey.flink.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * 对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，而且也可以借此收集用户的偏好信息。
 * 更加具体的应用是，
 * 我们可以根据用户的地理位置进行划分，从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。
 *
 * 存在有些城市，点击量非常高，且点击是同一个人，存在恶意刷单情况
 * 但是如果用户在一段时间非常频繁地点击广告，这显然不是一个正常行为，有刷点击量的嫌疑。
 */
public class Flink13_Project_AdCount_Window {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// csv中时间戳有序
        // 2.读取文本数据创建流，转换为JavaBean，提取时间戳生产WaterMark
        WatermarkStrategy<AdsClickLog> adsClickLogWatermarkStrategy = WatermarkStrategy.<AdsClickLog>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                    @Override
                    public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<AdsClickLog> adsClickLogDs = env.readTextFile("input/AdClickLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new AdsClickLog(Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            split[2],
                            split[3],
                            Long.valueOf(split[4]));

                })
                .assignTimestampsAndWatermarks(adsClickLogWatermarkStrategy);

        // 根据黑名单做过滤
        SingleOutputStreamOperator<AdsClickLog> filterDs = adsClickLogDs
                .keyBy(data -> data.getUserId() + "_" + data.getAdId())
                .process(new BlackListProcessFunc(100L));

        // 3.按照省份分组
        KeyedStream<AdsClickLog, String> provinceKeyedStream = filterDs.keyBy(AdsClickLog::getProvince);

        // 4.开窗聚合，增量聚合+窗口函数补充窗口信息
        SingleOutputStreamOperator<AdsClickCount> result = provinceKeyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdCountAggFunc(), new AdCountWindowFnc());

        // 5.打印结果
        result.print();
        filterDs.getSideOutput(new OutputTag<String>("BlackList"){}).print("告警数据");

        // 6.执行任务
        env.execute();
    }

    public static class AdCountAggFunc implements AggregateFunction<AdsClickLog, Integer, Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(AdsClickLog value, Integer accumulator) {
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

    public static class AdCountWindowFnc implements WindowFunction<Integer, AdsClickCount, String, TimeWindow>{
        @Override
        public void apply(String province, TimeWindow window, Iterable<Integer> input, Collector<AdsClickCount> out) throws Exception {

            Integer count = input.iterator().next();
            out.collect(new AdsClickCount(province,window.getEnd(),count));
        }
    }

    public static class BlackListProcessFunc extends KeyedProcessFunction<String, AdsClickLog, AdsClickLog>{
        // 定义最大的点击次数属性
        private Long maxClickCount;
        // 声明状态
        private ValueState<Long> countState;
        private ValueState<Boolean> isSendState;// 是否输出到侧输出流

        public BlackListProcessFunc(Long maxClickCount){
            this.maxClickCount = maxClickCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cout-state",Long.class));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSend-state",Boolean.class));
        }

        @Override
        public void processElement(AdsClickLog value, Context ctx, Collector<AdsClickLog> out) throws Exception {
            // 取出状态中的数据
            Long count = countState.value();
            Boolean isSend = isSendState.value();

            // 判断是第一条数据
            if(count == null){
                // 赋值为1
                countState.update(1L);
                // 注册第二天凌晨的定时器，用于清空状态,此处需要使用处理时间,东八区需要减掉8小时
                long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * (60 * 60 * 24 * 1000L) - 8 * 60 * 60 * 1000L;
//                System.out.println(new Timestamp(ts));
                ctx.timerService().registerProcessingTimeTimer(ts);
            }else {// 非第一条数据
                count = count + 1;
                // 更新状态
                countState.update(count);
                // 判断是否超过阈值
                if(count >= maxClickCount){
                    if(isSend == null){
                        // 报警信息进侧输出流，outputTag需要加{}，因为传入的是个匿名内部类
                        ctx.output(new OutputTag<String>("BlackList"){},
                                value.getUserId() + "点击了" + value.getAdId() + "广告达到" + maxClickCount
                                        + "次，存在恶意点击广告行为，报警！");

                        // 更新黑名单
                        isSendState.update(true);

                    }
                    return;
                }
            }
            // 正常输出数据
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
            isSendState.clear();
            countState.clear();
        }
    }
}
