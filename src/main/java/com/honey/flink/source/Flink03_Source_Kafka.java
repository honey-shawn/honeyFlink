package com.honey.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {

        // 0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Flink03_Source_Kafka");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 从kafka读取数据
        DataStreamSource<String> text = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));
        // 3. 打印数据
        text.print("kafka source");
        // 4. 执行任务
        env.execute();
    }
}
