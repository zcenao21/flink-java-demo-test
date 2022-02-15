package com.will.stream;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import com.will.table.SinkTest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Types;
import java.time.Duration;
import java.util.Random;

public class StateFlatmapTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> source = env.socketTextStream("localhost",9998);
        WatermarkStrategy<SinkTest.WC> strategy = WatermarkStrategy
                .<SinkTest.WC>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.timein);
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
                .flatMap(new Deduplicator())
                .print();
        env.execute("My Flink Job");
    }

    public static class Deduplicator extends RichFlatMapFunction<SinkTest.WC, SinkTest.WC>{
        ValueState<Boolean> keyHasBeenSeen;

        @Override
        public void open(Configuration conf){
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<Boolean>("keyHasBeenSeen", boolean.class);
            keyHasBeenSeen=getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(SinkTest.WC wc, Collector<SinkTest.WC> collector) throws Exception {
            if(keyHasBeenSeen.value()==null){
                collector.collect(wc);
                keyHasBeenSeen.update(true);
            }
        }
    }
}
