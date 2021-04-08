package com.honey.flink.state;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * BroadcastState-算子状态
 * 监控配置流变化之后，及时捕获最新配置值
 */
public class Flink09_State_BroadcastState_Op {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 从端口读取数据，并转换成javaBaen
        DataStreamSource<String> propertiesStream = env.socketTextStream("10.100.217.124", 9990);
        DataStreamSource<String> dataStream = env.socketTextStream("10.100.217.124", 9991);

        // 定义状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        BroadcastStream<String> broadcast = propertiesStream.broadcast(mapStateDescriptor);

        // 连接数据和广播
        BroadcastConnectedStream<String, String> connectedStream = dataStream.connect(broadcast);

        // 处理连接之后的流
        SingleOutputStreamOperator<String> result = connectedStream.process(new BroadcastProcessFunction<String, String, String>() {
            // 处理正常数据流dataStream
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String aSwitch = broadcastState.get("switch");

                if ("1".equals(aSwitch)) {
                    out.collect("读取了广播状态，切换1");
                } else if ("2".equals(aSwitch)) {
                    out.collect("读取了广播状态，切换2");
                } else {
                    out.collect("读取了广播状态，切换到其他");
                }
            }

            // 处理广播流broadcast
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                // 广播状态存储数据
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put("switch", value);
            }
        });

        result.print();

        env.execute();
    }

}
