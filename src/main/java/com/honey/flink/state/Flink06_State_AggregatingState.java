package com.honey.flink.state;

import com.honey.flink.bean.AvgVc;
import com.honey.flink.bean.WaterSensor;
import com.honey.flink.bean.WaterSensor2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * AggregatingState
 * 计算每个传感器的平均水位
 */
public class Flink06_State_AggregatingState {
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

        // 使用状态编程的方式实现平均水位
        SingleOutputStreamOperator<WaterSensor2> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor2>() {
            //定义状态
            private AggregatingState<Integer, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, AvgVc, Double>(
                        "agg-state", new AggregateFunction<Integer, AvgVc, Double>() {
                    @Override
                    public AvgVc createAccumulator() {
                        return new AvgVc(0, 0);
                    }

                    @Override
                    public AvgVc add(Integer value, AvgVc accumulator) {
                        return new AvgVc(accumulator.getVcSum() + value,
                                accumulator.getCount() + 1);
                    }

                    @Override
                    public Double getResult(AvgVc accumulator) {
                        return accumulator.getVcSum() * 1D / accumulator.getCount();
                    }

                    @Override
                    public AvgVc merge(AvgVc a, AvgVc b) {
                        return new AvgVc(a.getVcSum() + b.getVcSum(), a.getCount() + b.getCount());
                    }
                }
                        , AvgVc.class
                ));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor2> out) throws Exception {
                // 将当前数据累加近状态
                aggregatingState.add(value.getVc());

                // 取出状态中的数据
                Double avgVc = aggregatingState.get();

                // 输出数据
                out.collect(new WaterSensor2(value.getId(), value.getTs(), avgVc));
            }
        });


        result.print();

        env.execute();
    }
}
