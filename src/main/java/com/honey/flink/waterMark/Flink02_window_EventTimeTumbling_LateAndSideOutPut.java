package com.honey.flink.waterMark;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink02_window_EventTimeTumbling_LateAndSideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.socketTextStream("10.100.217.124", 9990)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        // 乱序waterMark，2秒延迟
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

        // 开窗，5秒滚动窗口，运行迟到数据，侧数据流
        // 当触发了窗口计算后, 会先计算当前的结果, 但是此时并不会关闭窗口.以后每来一条迟到数据, 则触发一次这条数据所在窗口计算(增量计算)
        // 窗口关闭时间：wartermark 超过了窗口结束时间+等待时间
        WindowedStream<WaterSensor, String, TimeWindow> window = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))  // 运行数据晚到2分钟
                .sideOutputLateData(new OutputTag<WaterSensor>("side") {
                });
        // 计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");
        // 获取侧数据流
        DataStream<WaterSensor> sideOutput = result.getSideOutput(new OutputTag<WaterSensor>("side") {
        });

        // 打印执行
        result.print();
        sideOutput.print("side");

        env.execute();
    }
}
