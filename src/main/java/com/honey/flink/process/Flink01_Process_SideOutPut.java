package com.honey.flink.process;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * processAPI：侧输出流
 */
public class Flink01_Process_SideOutPut {
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

        // 使用processFunction将数据分流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDs.process(new SplitProcessFunc());

        // 打印
        result.print("主流");
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOut") {
        });
        sideOutput.print("侧数据流");

        // 执行
        env.execute();

    }
    public static class SplitProcessFunc extends ProcessFunction<WaterSensor,WaterSensor>{
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            // 取出水位线
            Integer vc = value.getVc();

            // 根据水位线高低，分流
            if (vc >= 30){
                // 将数据输出至主流
                out.collect(value);
            }else{
                // 将数据输出至侧输出流
                ctx.output(
                        new OutputTag<Tuple2<String,Integer>>("SideOut"){},
                        new Tuple2<>(value.getId(),vc));
            }
        }
    }
}
