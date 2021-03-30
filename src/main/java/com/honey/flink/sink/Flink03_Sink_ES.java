package com.honey.flink.sink;

import com.honey.flink.common.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        // 读取端口数据并转换成javaBean
        SingleOutputStreamOperator<WaterSensor> mapDS = env.socketTextStream("10.100.217.124", 9990)
//        SingleOutputStreamOperator<WaterSensor> mapDS = env.readFile("input/sensor.txt"); // 无界流
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
        // 将数据写入到ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<WaterSensor>(httpHosts,new MyEsSinkFunc());
        // 批量提交参数，如果是无界流, 需要配置bulk的缓存
        waterSensorBuilder.setBulkFlushMaxActions(1);  //处理无界流是，需要设置，单次计算的数量，1代表来一条处理一条

        ElasticsearchSink<WaterSensor> build = waterSensorBuilder.build();

        DataStreamSink dataStreamSink = mapDS.addSink(build);
        // 执行任务
        env.execute();
    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<WaterSensor>{
        @Override
        public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            HashMap<String,String> source = new HashMap<>();
            source.put("ts",waterSensor.getTs().toString());
            source.put("vc",waterSensor.getVc().toString());

            // 创建请求
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor1")
                    .type("_doc")
                    .id(waterSensor.getId())   // 幂等
                    .source(source);
            // 写入ES
            requestIndexer.add(indexRequest);
        }
    }

}
