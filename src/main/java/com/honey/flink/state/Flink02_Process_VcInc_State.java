package com.honey.flink.state;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 监控水位传感器的水位值，如果水位值在10秒之内(processing time)连续上升，则报警。
 * 使用状态编程实现
 */
public class Flink02_Process_VcInc_State {
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
            // 定义状态
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;

            // 初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state",Integer.class,Integer.MIN_VALUE));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                // 取出状态数据
                Integer lastVc = vcState.value();
                Long timerTs = tsState.value();

                // 取出当前数据中的水位线
                Integer curVc = value.getVc();

                // 当水位上升并且timerTs为null的时候
                if (curVc >= lastVc && timerTs == null){
                    // 注册定时器
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                }else if( curVc <= lastVc && timerTs != null){
                    // 删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    tsState.clear();
                }
                // 更新上一次水位线状态
                vcState.update(curVc);
                // 输出数据
                out.collect(value);
            }

            // 定时器触发动作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("sideOut"){}
                ,ctx.getCurrentKey() + "连续10s没有下降！"
                        );
                // 清空定时器时间状态
                tsState.clear();
            }
        });

        result.print("主流");
        result.getSideOutput( new OutputTag<String>("sideOut") {}).print("侧输出流");

        env.execute();
    }
}
