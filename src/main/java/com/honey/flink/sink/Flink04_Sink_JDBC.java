package com.honey.flink.sink;

import com.honey.flink.common.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink04_Sink_JDBC {
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
        // 将数据装换为字符串写入到JDBC
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into sensor values(?, ?, ?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        // 给占位符赋值
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(1, waterSensor.getTs());
                        preparedStatement.setInt(1, waterSensor.getVc());
                    }
                },
                // 批量提交参数，无界流需要设置批量处理数量
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withPassword("000000")
                        .withUsername("root")
                        .build()
        );
        mapDS.addSink(jdbcSink);

        // 执行任务
        env.execute();
    }
}
