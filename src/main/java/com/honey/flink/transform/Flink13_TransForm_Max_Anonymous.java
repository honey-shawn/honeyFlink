package com.honey.flink.transform;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink13_TransForm_Max_Anonymous {
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
//        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc"); // 按照id groupby 取最大vc，其他字段取第一次出现时
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc",false);// 按照id groupby 取最大vc，其他信息按照最新数据。
        // 当两次vc一样时，还是取上次的其他信息，这是需要设置false，true表示取第一个, false表示取最后一个.

        // 4.打印
        result.print();

        env.execute();


    }
}
