package com.honey.flink.project;

import com.honey.flink.bean.ApacheLog;
import com.honey.flink.bean.ItemCount;
import com.honey.flink.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 基于服务器log的热门页面浏览量统计
 * 读取服务器日志中的每一行log，统计在一段时间内用户访问每一个url的次数，然后排序输出显示。
 * 具体做法为：每隔5秒，输出最近10分钟内访问量最多的前N个URL。
 *
 * bug1：改做法存在问题，当数据是实时乱序流时，同一个窗口中，只有一个key，因为状态被清空。
 * 解决办法：窗口关闭之后，清空状态
 *
 * bug2：在bug1修复之后，之前listState的状态管理暴露出问题，同一个Key输出时，会出现多条
 * 解决办法：换成MapState，详见Flink12_Project_HotUrl_window2
 *
 */
public class Flink11_Project_HotUrl_window {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、读取文本数据创建流并转换为JavaBean对象，提取时间戳生产WaterMark
        // 跟进最大乱序时间，设定延迟，因为后面迟到时间设置的1分钟，此处可以缩短成5秒
        WatermarkStrategy<ApacheLog> apacheLogWatermarkStrategy = WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog apacheLog, long l) {
                        return apacheLog.getTs();
                    }
                });
        // 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/apache.log");
//        DataStreamSource<String> streamSource = env.socketTextStream("10.100.217.124", 9990);
        SingleOutputStreamOperator<ApacheLog> apacheLogDs = streamSource.map(data -> {
            String[] split = data.split(" ");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            return new ApacheLog(split[0],
                    split[1],
                    sdf.parse(split[3]).getTime(),
                    split[5],
                    split[6]);
        }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(apacheLogWatermarkStrategy);

        // 3、转换为元祖，（url,1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> urlToOneDs = apacheLogDs.map(new MapFunction<ApacheLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(ApacheLog value) throws Exception {
                return new Tuple2<>(value.getUrl(), 1);
            }
        });
        // 4、按照url分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = urlToOneDs.keyBy(data -> data.f0);

        // 5、开窗聚合，增量 + 窗口函数,滑动窗口，窗口10分钟，滑动步长5秒
        SingleOutputStreamOperator<UrlCount> urlCountByWindowDs = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1)) // 允许迟到时间
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){}) // 侧输出流
                .aggregate(new HotUrlAggFunc(), new HotUrlWindowFunc());

        // 6、按照窗口信息重新分组
        KeyedStream<UrlCount, Long> windowEndKeyedStream = urlCountByWindowDs.keyBy(UrlCount::getWindowEnd);

        // 7、使用状态编程 + 定时器的方式，实现同一个窗口中所有数据的排序输出
        SingleOutputStreamOperator<String> result = windowEndKeyedStream.process(new HotUrlProcessFunc(5));

        // 8、打印数据
        result.print();

        // 9、执行任务
        env.execute();
    }
    public static class HotUrlAggFunc implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer accmulator) {
            return accmulator + 1;
        }

        @Override
        public Integer getResult(Integer accmulator) {
            return accmulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

     public static class HotUrlWindowFunc implements WindowFunction<Integer, UrlCount, String, TimeWindow>{
         @Override
         public void apply(String url, TimeWindow window, Iterable<Integer> input, Collector<UrlCount> out) throws Exception {
             Integer count = input.iterator().next();
             out.collect(new UrlCount(url, window.getEnd(), count));
         }
     }

     public static class HotUrlProcessFunc extends KeyedProcessFunction<Long, UrlCount, String>{

         private Integer topSize;

         public HotUrlProcessFunc(Integer topSize){
             this.topSize = topSize;
         }

         //声明状态
         private ListState<UrlCount> listState;

         @Override
         public void open(Configuration parameters) throws Exception {
             listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlCount>("list-state", UrlCount.class));
         }

         @Override
         public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            // 将当前数据放置状态
             listState.add(value);

             // 注册定时器
             ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);

             // 注册定时器，用于窗口关闭之后，清空状态
             ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 61001L);// waterMark延迟时间1s + 允许迟到时间1min + 1ms

         }

         @Override
         public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
             if(timestamp == ctx.getCurrentKey() + 61001L) {
                 listState.clear();// 窗口关闭之后，清空状态
                 return;
             }

             // 取出状态中的数据
             Iterator<UrlCount> iterator = listState.get().iterator();
             ArrayList<UrlCount> urlCounts = Lists.newArrayList(iterator);

             // 排序
             urlCounts.sort((a, b) -> b.getCount() - a.getCount());

            // 取topN数据
             StringBuilder stringBuilder = new StringBuilder();
             stringBuilder.append("----------")
                     .append(new Timestamp(timestamp - 1L))
                     .append("----------")
                     .append("\n");
             for (int i = 0; i < Math.min(topSize, urlCounts.size()); i++){
                 UrlCount urlCount = urlCounts.get(i);

                 stringBuilder.append("Top").append(i + 1);
                 stringBuilder.append("Url:").append(urlCount.getUrl());
                 stringBuilder.append("Count:").append(urlCount.getCount());
                 stringBuilder.append("\n");
             }
             stringBuilder.append("----------")
                     .append(new Timestamp(timestamp - 1000L))
                     .append("----------")
                     .append("\n")
                     .append("\n");

             // 状态清空，放在窗口关闭之后
//             listState.clear();
             out.collect(stringBuilder.toString());

             // 休眠
             Thread.sleep(2000);

         }
     }
}
