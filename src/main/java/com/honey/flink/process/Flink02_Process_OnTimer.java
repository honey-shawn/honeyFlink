package com.honey.flink.process;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * processAPI：基于处理时间的定时器
 */
public class Flink02_Process_OnTimer {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从端口读取数据，并转换成javaBaen
        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.socketTextStream("10.100.217.124", 9990)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        // 使用processFunction的定时器功能，定时器只能在keyBy中处理
        waterSensorDs.keyBy(WaterSensor::getId)
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                // 获取档期数据的处理时间
                long ts = ctx.timerService().currentProcessingTime();
                System.out.println(ts);
                // 注册定时器，1秒
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L); // 1秒定时器
                // 输出数据
                out.collect(value);
            }

            // 触发定时器时，执行命令
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("定时器触发");

                // 贪睡模式，定时器触发之后，定下一个闹钟
                long ts = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L); // 1秒定时器
            }
        });
    }
}
