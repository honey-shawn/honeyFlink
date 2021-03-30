package com.honey.flink.transform;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 作用：一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
 * 参数：interface ReduceFunction<T>
 * 返回：KeyedStream -> SingleOutputStreamOperator
 * 聚合后结果的类型, 必须和原来流中元素的类型保持一致!
 */
public class Flink14_TransForm_reduce_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取端口数据并转换成JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("10.100.217.124", 9990)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        // 2.按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });

        // 3.计算最高水位线
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(
                        value1.getId(),     // id
                        value2.getTs(),     // 取最新的时间戳
                        Math.max(value1.getVc(),value2.getVc()) // 取最大值
                );
            }
        });
        // 4.打印
        result.print();

        // lambda写法
//        keyedStream
//                .reduce((value1,value2) -> {
//                    return new WaterSensor(value1.getId(),value2.getTs(),Math.max(value1.getVc(),value2.getVc()));
//                })
//                .print();

        env.execute();

    }

}
