package com.honey.flink.state;

import com.honey.flink.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * ListState
 * 针对每个传感器输出最高的3个水位值
 */
public class Flink04_State_ListState {
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

        // 使用listState实现每个传感器最高的三个水位线
        SingleOutputStreamOperator<List<WaterSensor>> result = keyedStream.map(new RichMapFunction<WaterSensor, List<WaterSensor>>() {
            // 定义状态
            private ListState<WaterSensor> top3State;

            @Override
            public void open(Configuration parameters) throws Exception {
                top3State = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor>("list-state", WaterSensor.class));
            }

            @Override
            public List<WaterSensor> map(WaterSensor value) throws Exception {
                // 将当前数据加入状态
                top3State.add(value);

                // 取出状态中的数据并排序
                ArrayList<WaterSensor> waterSensors = Lists.newArrayList(top3State.get().iterator());
                waterSensors.sort((o1, o2) -> o2.getVc() - o1.getVc());

                //判断当前数据是否超过三天，如果超过则删除最后一条
                if (waterSensors.size() > 3) {
                    waterSensors.remove(3);
                }

                // 更新状态
                top3State.update(waterSensors);

                return waterSensors;
            }
        });

        result.print();

        env.execute();
    }
}
