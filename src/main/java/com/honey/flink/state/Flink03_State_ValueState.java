package com.honey.flink.state;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * valueState
 * 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
 */
public class Flink03_State_ValueState {
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

        // 使用RichFunction实现水位线跳变报警需求
        SingleOutputStreamOperator<String> result = keyedStream.flatMap(new RichFlatMapFunction<WaterSensor, String>() {
            // 定义状态
            private ValueState<Integer> vcState;

            // 初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                // 获取状态中的数据
                Integer lastVc = vcState.value();

                // 更新状态
                vcState.update(value.getVc());

                // 当上一次水位部位null，且出现跳变的时候进行报警
                if (lastVc != null && Math.abs(lastVc - value.getVc()) > 10) { // && 先判断第一个条件，false直接跳出
                    out.collect(value.getId() + "出现水位跳变！");
                }
            }
        });

        result.print();

        env.execute();
    }
}
