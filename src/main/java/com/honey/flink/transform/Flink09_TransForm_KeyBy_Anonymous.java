package com.honey.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyBy:把流中的数据分到不同的分区中.具有相同key的元素会分到同一个分区中.一个分区中可以有多重不同的key.在内部是使用的hash分区来实现的.
 * 参数：Key选择器函数: interface KeySelector<IN, KEY>
 * 返回：DataStream → KeyedStream
 * 什么值不可以作为KeySelector的Key:
 * 	没有覆写hashCode方法的POJO, 而是依赖Object的hashCode. 因为这样分组没有任何的意义: 每个元素都会得到一个独立无二的组.
 *      实际情况是:可以运行, 但是分的组没有意义.
 * 	任何类型的数组
 *
 */
public class Flink09_TransForm_KeyBy_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                })
                .print();
        env.execute();

        // lambda表达式
//        env.fromElements(10, 3, 5, 9, 20, 8)
//                .keyBy(value -> value % 2 == 0 ? "偶数" : "奇数")
//                .print();
    }
}