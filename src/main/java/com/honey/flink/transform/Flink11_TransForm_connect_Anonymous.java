package com.honey.flink.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect:连接两个保持他们类型的数据流，两个数据流被connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
 * 参数：另外一个流
 * 返回：DataStream[A], DataStream[B] -> ConnectedStreams[A,B]
 * 注意：
 * 1.	两个流中存储的数据类型可以不同
 * 2.	只是机械的合并在一起, 内部仍然是分离的2个流
 * 3.	只能2个流进行connect, 不能有第3个参与
 */
public class Flink11_TransForm_connect_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");
        // 把两个流连接在一起: 貌合神离
        ConnectedStreams<String, Integer> connectedStreams = stringStream.connect(intStream);
        // 处理连接之后的流
        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String s) throws Exception {
                return s;
            }

            @Override
            public Object map2(Integer integer) throws Exception {
                return integer;
            }
        });

        //打印
//        connectedStreams.getFirstInput().print("first");
//        connectedStreams.getSecondInput().print("second");
        result.print();

        // 执行
        env.execute();

    }
}
