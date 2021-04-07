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

/**
 * 多并行度下，waterMark的传递机制
 */
public class Flink07_window_EventTimeTumbling_WaterMarkTrans {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 提取时间戳
        WatermarkStrategy<String> waterSensorWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String waterSensor, long l) {
                        String[] split = waterSensor.split(",");
                        return Long.parseLong(split[1]) * 1000L;
                    }
                });
        // 端口读数据，转换成javaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.socketTextStream("10.100.217.124", 9990)
                // 设置WatermarkStrategy
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });


        // 按照id分组
        KeyedStream<WaterSensor, String> KeyedStream = waterSensorDs.keyBy(WaterSensor::getId);

        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        // 计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");

        // 打印执行
        result.print();

        env.execute();

    }
}
