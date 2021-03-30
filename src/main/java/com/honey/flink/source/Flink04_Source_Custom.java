package com.honey.flink.source;

import com.honey.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 获取数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource("10.100.217.123", 9990));
        // 3. 打印数据
        streamSource.print();
        // 4. 执行
        env.execute();
    }

    // 实现一个从socket读取数据的source
    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket = null;
        private BufferedReader reader = null;

        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // 创建输入流
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            // 输入数据
            String line = reader.readLine();
            while (isRunning && line != null) {
                String[] split = line.split(",");
                ctx.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
                line = reader.readLine();
            }
        }

        /**
         * 大多数的source在run方法内部都会有一个while循环,
         * 当调用这个方法的时候, 应该可以让run方法中的while循环结束
         */

        @Override
        public void cancel() {
            isRunning = false;
            try {
                reader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
/*
sensor_1,1607527992000,20
sensor_1,1607527993000,40
sensor_1,1607527994000,50
 */
