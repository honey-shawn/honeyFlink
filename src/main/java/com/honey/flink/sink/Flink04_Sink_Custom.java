package com.honey.flink.sink;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class Flink04_Sink_Custom {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        // 读取端口数据并转换成javaBean
        SingleOutputStreamOperator<WaterSensor> mapDS = env.socketTextStream("10.100.217.124", 9990)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(
                                split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2])
                        );
                    }
                });
        // 将数据装换为字符串写入到Mysql
        mapDS.addSink(new MySink());

        // 执行任务
        env.execute();
    }
    public static class MySink extends RichSinkFunction<WaterSensor> {
        // 链接
        private Connection connection;
        private PreparedStatement preparedStatement;

        // 生命周期方法，用于创建链接
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "000000");
            preparedStatement = connection.prepareStatement("insert into sensor values(?, ?, ?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            // 给占位符赋值
            preparedStatement.setString(1,value.getId());
            preparedStatement.setLong(1,value.getTs());
            preparedStatement.setInt(1,value.getVc());

            // 执行
            preparedStatement.execute();
        }

        // 生命周期方法，用于关闭链接
        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
