package com.will.stream;

import com.will.table.SinkTest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Random;

public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> source = env.socketTextStream("localhost",9998);
        WatermarkStrategy<SinkTest.WC> strategy = WatermarkStrategy
                .<SinkTest.WC>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.timein);
        Random rand = new Random();
        source
                .flatMap(new FlatMapFunction<String, SinkTest.WC>() {
                    @Override
                    public void flatMap(String s, Collector<SinkTest.WC> collector) throws Exception {
                        String[] ss = s.split("[,:\\s+()]");
                        for(String in: ss){
                            collector.collect(new SinkTest.WC(in,1 ,System.currentTimeMillis()));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(wc->wc.word)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .process(new MyWastefulMax())
                .print();
        env.execute("My Flink Job");
    }

    public static class MyWastefulMax extends ProcessWindowFunction<SinkTest.WC, Tuple3<String,Long,Integer>,String, TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<SinkTest.WC> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
            int sum=0;
            for(SinkTest.WC wc:iterable){
                sum=(int)wc.frequency+sum;
            }
            collector.collect(Tuple3.of(s,context.window().getEnd(),sum));
        }
    }
}
