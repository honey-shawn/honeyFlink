package com.honey.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从端口获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("10.100.217.123", 9990);

        // 3.打印数据
        socketTextStream.print();

        //4.执行
        env.execute();

    }
}
