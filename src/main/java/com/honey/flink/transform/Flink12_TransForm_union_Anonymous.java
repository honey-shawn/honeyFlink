package com.honey.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union:对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream
 * 与connect的区别：
 * 1.	union之前两个流的类型必须是一样，connect可以不一样
 * 2.	connect只能操作两个流，union可以操作多个。
 *
 */
public class Flink12_TransForm_union_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

        // 把多个流union在一起成为一个流, 这些流中存储的数据类型必须一样: 水乳交融
        DataStream<Integer> union = stream1.union(stream2);
//                .union(stream3)
//                .print();
        // 打印
        union.print();

        env.execute();

    }
}
