package com.honey.flink.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  map:将数据流中的数据进行转换, 形成新的数据流，消费一个元素并产出一个元素
 *  参数：lambda表达式或MapFunction实现类
 *  结果：DataStream → DataStream
 */
public class Flink05_TransForm_Map_Anonymous {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        // 转化
        SingleOutputStreamOperator<Integer> map = integerDataStreamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * value;
            }
        });
        // 打印
        map.print();
        // 执行
        env.execute();

        // lambda表达式
//        env.fromElements(1,2,3,4)
//                .map(ele -> ele * ele)
//                .print();
    }
}
