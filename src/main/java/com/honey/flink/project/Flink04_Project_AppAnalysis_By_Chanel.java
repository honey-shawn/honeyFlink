package com.honey.flink.project;

import com.honey.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 *APP市场推广统计 - 分渠道
 * 渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）
 */
public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从自定义source中加载数据
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDs = env.addSource(new AppMarketingDataSource());
        // 按照渠道+行为分组
        KeyedStream<MarketingUserBehavior, Tuple2<String, String>> keyedStream = marketingUserBehaviorDs.keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                return new Tuple2<String, String>(value.getChannel(), value.getBehavior());
            }
        });
        // 计算总和
        SingleOutputStreamOperator<Tuple2<Tuple2<String, String>, Integer>> result = keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, MarketingUserBehavior, Tuple2<Tuple2<String, String>, Integer>>() {
            private HashMap<String, Integer> hashMap = new HashMap<>();

            @Override
            public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<Tuple2<String, String>, Integer>> out) throws Exception {
                // 拼接hashKey
                String hashKey = value.getChannel() + "_" + value.getBehavior();
                // 取出HashMap中是数量，如果数据是第一次过来，则给定默认值0
                Integer count = hashMap.getOrDefault(hashKey, 0);
                count++;
                // 输出结果
                out.collect(new Tuple2<>(ctx.getCurrentKey(), count));
                // 更新hashMap中的值
                hashMap.put(hashKey, count);
            }
        });

        result.print();


//        env.addSource(new AppMarketingDataSource())
//                .map(behavior -> Tuple2.of(behavior.getChannel() + "_" + behavior.getBehavior(),1L))
//                .returns(Types.TUPLE(Types.STRING,Types.LONG))
//                .keyBy(t -> t.f0)
//                .sum(1)
//                .print();


        env.execute();

    }

    // 自定义source
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }

}
