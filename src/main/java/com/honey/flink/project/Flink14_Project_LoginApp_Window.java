package com.honey.flink.project;

import com.honey.flink.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Flink14_Project_LoginApp_Window {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取文本数据，转换为JavaBean，提取时间戳生产Watermark
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<LoginEvent> loginEventDs = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000L);
                }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        // 3.按照用户ID分组
        KeyedStream<LoginEvent, Long> userIdKeyedStream = loginEventDs.keyBy(LoginEvent::getUserId);

        // 4.使用ProcessAPI，状态，定时器
        SingleOutputStreamOperator<String> result = userIdKeyedStream.process(new LoginKeyedProcessFunction(2, 2));

        // 5.打印
        result.print();

        // 6.执行任务
        env.execute();
    }

    public static class LoginKeyedProcessFunction extends KeyedProcessFunction<Long, LoginEvent, String> {
        // 定义属性信息
        private Integer ts;
        private Integer count;

        public LoginKeyedProcessFunction(Integer ts, Integer count){
            this.ts = ts;
            this.count = count;
        }

        // 声明状态
        private ListState<LoginEvent> loginEventListState;
        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            Long timeTs = valueState.value();

            // 取出时间类型
            String eventType = value.getEventType();

            // 判断是否为第一条失败数据，则需要注册定时器
            if ("fail".equals(eventType)) {
                if (!iterator.hasNext()) { // 第一条失败数据
                    // 注册定时器
                    long curtTs = ctx.timerService().currentWatermark() + ts * 1000L;
                    ctx.timerService().registerEventTimeTimer(curtTs);
                    // 更新时间状态
                    valueState.update(curtTs);
                }
                // 将当前失败数据加入到状态
                loginEventListState.add(value);
            } else {
                // 说明已经注册过定时器
                if (timeTs != null) {
                    ctx.timerService().deleteEventTimeTimer(timeTs);
                }
                // 成功数据，清空List并删除定时器
                loginEventListState.clear();
                ctx.timerService().deleteEventTimeTimer(timeTs);
                valueState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);
            // 判断连续失败次数
            if(loginEvents.size() >= count){
                LoginEvent first = loginEvents.get(0);
                LoginEvent last = loginEvents.get(loginEvents.size() - 1);

                out.collect(first.getUserId()
                        + "用户在"
                        + first.getEventTime()
                        + "到"
                        + last.getEventTime()
                        + "之间，连续登录失败了"
                        + loginEvents.size()
                        + "次！"
                );
            }
            // 清空状态
            loginEventListState.clear();
            valueState.clear();
        }
    }
}
