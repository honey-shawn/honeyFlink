package com.honey.flink.state;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 键控状态
 */
public class Flink01_State_KeyedState {
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

        // 状态的使用
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new MyStateProcessFunc());

        result.print();

        env.execute();
    }
    public static class MyStateProcessFunc extends KeyedProcessFunction<String,WaterSensor,WaterSensor>{
        // 1、定义状态
        private ValueState<Long> valueState;
        private ListState<Long> longListState;
        private MapState<String,Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor,WaterSensor> aggregatingState;

        // 2、初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state",Long.class));
            longListState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state",Long.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state",String.class,Long.class));
//            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>();
//            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Object, WaterSensor>());
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            // 3、状态的使用
            // 3.1 value的状态
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();
            // 3.2 ListState
            Iterable<Long> longs = longListState.get();
            longListState.add(122L);
            longListState.clear();
            longListState.update(new ArrayList<>());
            // 3.3 Map状态
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            mapState.get("");
            mapState.contains("");
            mapState.put("",122L);
            mapState.putAll(new HashMap<>());
            mapState.remove("");
            mapState.clear();
            // 3.4 reduce状态
            WaterSensor waterSensor = reducingState.get();
            reducingState.add(new WaterSensor());
            reducingState.clear();
            // 3.5 Agg 状态
            aggregatingState.add(value);
            WaterSensor waterSensor1 = aggregatingState.get();
            aggregatingState.clear();


        }
    }
}
