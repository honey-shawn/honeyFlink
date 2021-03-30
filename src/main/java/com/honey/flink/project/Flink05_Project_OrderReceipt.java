package com.honey.flink.project;

import com.honey.flink.bean.OrderEvent;
import com.honey.flink.bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;

/**
 * 来自两条流的订单交易匹配,对账
 */
public class Flink05_Project_OrderReceipt {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1. 读取Order流并转换成javabean
        DataStreamSource<String> orderStreamDs = env.readTextFile("input/OrderLog.csv");
        // 2. 读取交易流并转换成javabean
        DataStreamSource<String> receiptStreamDs = env.readTextFile("input/ReceiptLog.csv");

        // 转换成javaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDs = orderStreamDs.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
                //过滤出所有支付订单
                if ("pay".equals(orderEvent.getEventType())) {
                    out.collect(orderEvent);
                }
            }
        });
        SingleOutputStreamOperator<TxEvent> txDs = receiptStreamDs.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        // 为了将同一个id放在同一个并行度里面计算，按照txid进行分组,keyBy主要是对数据进行重分区，对数据不产生影响
        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = orderEventDs.keyBy(OrderEvent::getTxId);
        KeyedStream<TxEvent, String> txEventStringKeyedStream = txDs.keyBy(TxEvent::getTxId);

        // 链接两个流
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventStringKeyedStream.connect(txEventStringKeyedStream);

        // 处理两条流的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = connectedStreams.process(new MyCoKeyedProcessFunc());

        // 打印
        result.print();

        env.execute();

    }
    public static class MyCoKeyedProcessFunc extends KeyedCoProcessFunction<String,OrderEvent,TxEvent, Tuple2<OrderEvent,TxEvent>>{
        private HashMap<String,OrderEvent> orderEventHashMap = new HashMap<>();
        private HashMap<String,TxEvent> txEventHashMap = new HashMap<>();

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            if(txEventHashMap.containsKey(value.getTxId())){
                TxEvent txEvent = txEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(value,txEvent));
            }else{
                orderEventHashMap.put(value.getTxId(),value);
            }
        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            if(orderEventHashMap.containsKey(value.getTxId())){
                OrderEvent orderEvent = orderEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(orderEvent,value));
            }else{
                txEventHashMap.put(value.getTxId(),value);
            }
        }
    }

}
