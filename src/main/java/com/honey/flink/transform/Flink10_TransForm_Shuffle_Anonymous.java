package com.honey.flink.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * shuffle:把流中的元素随机打乱. 对同一个组数据, 每次只需得到的结果都不同.
 * 参数：无
 * 返回：DataStream → DataStream
 */
public class Flink10_TransForm_Shuffle_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(10, 3, 5, 9, 20, 8)
                .shuffle()
                .print();
        env.execute();
    }
}