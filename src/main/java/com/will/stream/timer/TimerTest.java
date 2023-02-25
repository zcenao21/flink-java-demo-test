package com.will.stream.timer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.text.SimpleDateFormat;

public class TimerTest {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 设置自动产生输入，并设置输入格式
        DataStream<Stock> source = env
                .addSource(new SimulateKafkaSource());

        // 输入打印
        source.print();

        source
            .keyBy((event)->event.stockName)
            .process(new StockProcessFunction())
            .print();

        env.execute("My Timer Test");
    }
}
