package com.honey.flink.state;

import com.honey.flink.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ListState-算子状态
 */
public class Flink08_State_ListState_Op {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从端口读取数据，并转换成javaBaen
        SingleOutputStreamOperator<WaterSensor> waterSensorDs = env.socketTextStream("10.100.217.124", 9990)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        // 统计元素的个数
        SingleOutputStreamOperator<Integer> result = waterSensorDs.map(new MyMapFunc());

        result.print();

        env.execute();
    }
    public static class MyMapFunc implements MapFunction<WaterSensor,Integer>, CheckpointedFunction{

        // 定义状态
        private ListState<Integer> listState;
        private Integer count = 0;

        // 初始化
        // 初始化时会调用这个方法，向本地状态中填充数据. 每个子任务调用一次
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Integer>("state",Integer.class));
            // 防止任务失败重启，从状态中读取状态
            Iterator<Integer> iterator = listState.get().iterator();
            while (iterator.hasNext()){
                count+=iterator.next();
            }
        }

        @Override
        public Integer map(WaterSensor value) throws Exception {
            count++;
            return count;
        }

        // Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(count);
        }

    }
}
