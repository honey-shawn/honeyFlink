package com.honey.flink.transform;

import com.honey.flink.common.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink16_TransForm_Repartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("10.100.217.124", 9990);

        // 使用不同的重分区策略分区后打印
        socketTextStream.keyBy(data -> data).print("keyBy"); // 同一个数据在同一个分区
        socketTextStream.shuffle().print("shuffle");  // 随机
        socketTextStream.rebalance().print("rebalance"); // 均衡
        socketTextStream.rescale().print("rescale");
        socketTextStream.global().print("global");
//        socketTextStream.forward().print();// 报错
        socketTextStream.broadcast().print("broadcast");

        // 对比rebalance与rescale的区别
        env.setParallelism(1);
        SingleOutputStreamOperator<String> map = socketTextStream.map(x -> x).setParallelism(2);
        map.print();
        map.rebalance().print();
        map.rescale().print();





        env.execute();


    }
}
