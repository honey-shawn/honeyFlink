package com.honey.flink.waterMark;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class Flink01_window_EventTimeTumbling {
    public static void main(String[] args) throws Exception {
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
        // 有序waterMark
        // 提取数据中的时间戳字段，自增的设置waterMark，计算窗口为：[0,5),[5,10)
//        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
//            @Override
//            public long extractTimestamp(WaterSensor waterSensor, long l) {
//                return waterSensor.getTs() * 1000L;
//            }
//        });
        // 乱序waterMark
        // 延迟2秒关闭窗口(允许迟到2秒)，数据计算还是按照5秒计算
        // 输入事件时间：1,2,3,5,1,2,9  输出：求和数5，迟到的1,2都能计算到，5,9不在窗口中
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs() * 1000L;
                    }
                });
        // 设置waterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDs.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        // 按照id分组
        KeyedStream<WaterSensor, String> KeyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        // 计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");

        // 打印执行
        result.print();

        env.execute();





    }
}
