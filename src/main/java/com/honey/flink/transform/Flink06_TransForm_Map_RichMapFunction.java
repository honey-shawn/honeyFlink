package com.honey.flink.transform;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 所有Flink函数类都有其Rich版本。
 * 它与常规函数的不同在于，
 * 1、可以获取运行环境的上下文，
 * 2、并拥有一些生命周期方法，做状态编程
 *
 */
public class Flink06_TransForm_Map_RichMapFunction {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取数据并处理，和打印
        env.fromElements(1,2,3,4,5)
                .map(new MyRichMapFunction())
//                .setParallelism(2)
                .print();
        // 执行
        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<Integer, Integer> {
        // 默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open ... 执行一次");
        }

        // 默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次
        @Override
        public void close() throws Exception {
            System.out.println("close ... 执行一次");
        }

        @Override
        public Integer map(Integer value) throws Exception {
            System.out.println("map ... 一个元素执行一次");
            return value * value;
        }

    }
}
