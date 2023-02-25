package com.will.stream.watermark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

public class WatermarkTest {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().setAutoWatermarkInterval(500);
        // 设置自动产生输入，并设置输入格式
        DataStream<String> source = env
                .socketTextStream("localhost",9999);


        // 解析输入，三列分别表示输入字符串，事件时间，水印时间
        DataStream<String> stream = source
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        collector.collect(s);
                    }
                });

        stream
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
            .process(new ProcessAllWindowFunction<String, Object, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<String, Object, TimeWindow>.Context context, Iterable<String> iterable, Collector<Object> collector) throws Exception {
                    collector.collect(context.window()+":"+iterable);
                }
            })
            .print();


        env.execute("My WaterMark Test");
    }
}
