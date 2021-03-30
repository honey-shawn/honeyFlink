package com.honey.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("input/word.txt");

        // 3.压平
        FlatMapOperator<String,String> wordDS = lineDS.flatMap(new MyFlatMapFunc());
        
        //4.将单词转换为元组
        MapOperator<String,Tuple2<String,Integer>> wordToOneDs = wordDS.map(new MapFunction<String, Tuple2<String,Integer>>() {
            public Tuple2<String, Integer> map(String value) throws Exception {
//                return new Tuple2<String, Integer>(value,1);
                return  Tuple2.of(value,1);
            }
        });

        // 5.分组
        UnsortedGrouping<Tuple2<String,Integer>> groupBy = wordToOneDs.groupBy(0);

        // 6.聚合
        AggregateOperator<Tuple2<String,Integer>> result = groupBy.sum(1);

        // 7.打印结果
        result.print();


        // 3. 转换数据格式
//        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
//                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//                    String[] split = line.split(" ");
//                    for (String word : split) {
//                        out.collect(Tuple2.of(word, 1L));
//                    }
//                })
//                .returns(Types.TUPLE(STRING, Types.LONG));  //当Lambda表达式使用 java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息

        // 4. 按照 word 进行分组
//        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
        // 5. 分组内聚合统计
//        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        // 6. 打印结果
//        sum.print();

    }

//    自定义实现压平操作的类
    public static class MyFlatMapFunc implements FlatMapFunction<String,String>{

        public void flatMap(String value, Collector<String> out) throws Exception {
            // 按照空格切分
            String[] words = value.split(" ");

            // 遍历words，写出单词
            for (String word : words){
                out.collect(word);
            }
        }
    }
}
