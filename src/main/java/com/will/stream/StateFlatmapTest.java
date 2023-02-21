package com.will.stream;

import com.will.table.SinkTest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class StateFlatmapTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> source = env.socketTextStream("localhost",9998);
        WatermarkStrategy<WC> strategy = WatermarkStrategy
                .<WC>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.timeIn);

        SingleOutputStreamOperator<WC> stream= source
                .flatMap(new FlatMapFunction<String, WC>() {
                    @Override
                    public void flatMap(String s, Collector<WC> collector) throws Exception {
                        String[] ss = s.split("[,:\\s+()]");
                        for(String in: ss){
                            collector.collect(new WC(in,1 ,System.currentTimeMillis()));
                        }
                    }
                });

        stream
            .assignTimestampsAndWatermarks(strategy)
            .keyBy(wc->wc.word)
            .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
            .sum("frequency")
            .map(new MapFunction<WC, Object>() {
                @Override
                public Object map(WC wc) throws Exception {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

                    wc.formattedTimeIn = wc.formattedTimeIn + " | " + sdf.format(System.currentTimeMillis());
                    return wc;
                }
            })
            .print();

        stream
                .map(new MapFunction<WC, Object>() {
                    @Override
                    public Object map(WC wc) throws Exception {
                        wc.word=wc.word+"========================================================";
                        return wc;
                    }
                }).print();

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
            }else{
                collector.collect(wc);
            }
        }
    }

    public static class WC {
        public String word;//hello
        public long frequency;//1

        public long timeIn;

        public String formattedTimeIn;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency, long timein) {
            this.word = word;
            this.frequency = frequency;
            this.timeIn=timein;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
            this.formattedTimeIn=sdf.format(timein);
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency+" "+formattedTimeIn;
        }
    }
}
