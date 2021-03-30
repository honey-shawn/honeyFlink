package com.honey.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * filter:根据指定的规则将满足条件（true）的数据保留，不满足条件(false)的数据丢弃
 * 参数：FlatMapFunction实现类
 * 返回：DataStream → DataStream
 */
public class Flink08_TransForm_Filter_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(10,3,5,9,20,8)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2 == 0;
                    }
                })
                .print();
        env.execute();

        // lambda表达式
//        env.fromElements(10,3,5,9,20,8)
//                .filter(value -> value % 2 ==0)
//                .print();
    }
}
