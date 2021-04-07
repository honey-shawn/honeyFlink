package com.honey.flink.process;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 监控水位传感器的水位值，如果水位值在10秒之内(processing time)连续上升，则报警。
 */
public class Flink03_Process_VcInc {
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

        // 按照传感器id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDs.keyBy(WaterSensor::getId);

        // 使用processFunction实现连续时间内水位不下降，则报警，且将报警信息输出到侧数据流
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private Integer lastVc = Integer.MIN_VALUE; // 上次水位
            private Long timerTs = Long.MIN_VALUE; // 注册定时器的时间

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                // 取出水位线
                Integer vc = value.getVc();
                // 将当前水位线与上一次值进行比较
                if (vc > lastVc && timerTs == Long.MIN_VALUE) {
                    // 注册定时器
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    System.out.println("注册定时器：" + ts);
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    // 更新上一次的水位线值,更新定时器的时间戳
                    lastVc = vc;
                    timerTs = ts;
                } else if (vc < lastVc) {
                    // 删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    System.out.println("删除定时器：" + timerTs);

                    // 置位初始状态
                    timerTs = Long.MIN_VALUE;
                }
                lastVc = vc;  // 更新上一次的水位线值
                // 输出数据
                out.collect(value);
            }

            // 定时器触发动作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(
                        new OutputTag<String>("sideOut") {
                        }
                        , ctx.getCurrentKey() + "连续10s没有下降"
                );
                // 置位初始状态
                timerTs = Long.MIN_VALUE;
            }
        });

        result.print("主流");
        result.getSideOutput( new OutputTag<String>("sideOut") {}).print("侧输出流");

        env.execute();
    }
}
