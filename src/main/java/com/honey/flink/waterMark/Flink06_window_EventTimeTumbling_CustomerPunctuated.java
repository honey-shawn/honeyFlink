package com.honey.flink.waterMark;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 自定义间歇性WatermarkStrategy
 */
public class Flink06_window_EventTimeTumbling_CustomerPunctuated {
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
        // 提取数据中的时间戳
        WatermarkStrategy<WaterSensor> watermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPunt(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });
        // 设置waterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDs
                .assignTimestampsAndWatermarks(watermarkStrategy);

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
    // 自定义周期性的waterMark生成器
    public static class MyPunt implements WatermarkGenerator<WaterSensor>{

        private Long maxTs;
        // 允许的最大延迟时间 ms
        private Long maxDelay;

        public MyPunt(Long maxDelay){
            this.maxDelay = maxDelay * 1000L;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }

        // 每收到一个元素, 执行一次. 用来生产WaterMark中的时间戳
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent..." + eventTimestamp);
            //有了新的元素找到最大的时间戳
            maxTs = Math.max(maxTs,eventTimestamp);
            System.out.println(maxTs);
            // 生产waterMark
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));
        }
        // 周期性的把WaterMark发射出去, 默认周期是200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
