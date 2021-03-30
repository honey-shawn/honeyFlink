package com.honey.flink.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  flatMap:消费一个元素并产生零个或多个元素
 *  参数：lambda表达式或FlatMapFunction实现类
 *  结果：DataStream → DataStream
 */
public class Flink07_TransForm_FlatMap_Anonymous {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        // 转化
        SingleOutputStreamOperator<Integer> flatmap = integerDataStreamSource.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                out.collect(value * value);
                out.collect(value * value * value);
            }
        });
        // 打印
        flatmap.print();
        // 执行
        env.execute();

        // lambda表达式
//        env.fromElements(1,2,3,4)
//                .flatMap((Integer value, Collector<Integer> out) -> {
//                    out.collect(value * value);
//                    out.collect(value*value*value);
//                }).returns(Types.INT)
//        在使用Lambda表达式表达式的时候, 由于泛型擦除的存在, 在运行的时候无法获取泛型的具体类型, 全部当做Object来处理, 及其低效,
//        所以Flink要求当参数中有泛型的时候, 必须明确指定泛型的类型.
//                .print();
    }
}
