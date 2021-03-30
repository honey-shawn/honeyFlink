package com.honey.flink.sink;


import com.honey.flink.common.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Flink02_Sink_Redis {
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
        // 将数据写入到Redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();
        DataStreamSink dataStreamSink = mapDS.addSink(new RedisSink<>(jedisPoolConfig, new MyRedisMapper()));
        // 执行任务
        env.execute();
    }
    public static class MyRedisMapper implements RedisMapper<WaterSensor>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 返回存在Redis中的数据类型  存储的是Hash, 第二个参数是外面的key
            return new RedisCommandDescription(RedisCommand.HSET,"Sensor");
        }

        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            // 从数据中获取Key: Hash的Key
            return waterSensor.getId();
        }

        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            // 从数据中获取Value: Hash的value
            return waterSensor.getVc().toString();
        }
    }

}
