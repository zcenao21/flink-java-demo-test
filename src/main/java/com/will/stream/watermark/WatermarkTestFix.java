package com.will.stream.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;

public class WatermarkTestFix {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 设置自动产生输入，并设置输入格式
        DataStream<String> source = env
                .socketTextStream("localhost",9999);


        // 解析输入，三列分别表示输入字符串，事件时间，水印时间
        DataStream<Tuple2<String, Long>> stream = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] ss = s.split(",");
                        Tuple2<String, Long> input = new Tuple2<String, Long>();
                        try{
                            input.f0 = ss[0];
                            input.f1 = sdf.parse(ss[1]).getTime();
                        }catch (Exception e){
                            System.out.println("cant not parse " + input);
                            input.f0 = "";
                            input.f1= 0L;
                        }
                        collector.collect(input);
                    }
                });

        stream.map(new MapFunction<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object map(Tuple2<String, Long> input) throws Exception {
                        return new Tuple2<>(input.f0, sdf.format(input.f1));
                    }
                })
            .print("==================================================== input ");

        OutputTag<Tuple2<String, Long>> lateTag = new OutputTag<Tuple2<String, Long>>("late"){};
        SingleOutputStreamOperator<Object> streamOut = stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                    .withTimestampAssigner((event,timestamp)->event.f1))
            .setParallelism(1)
            .keyBy(event->event.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .sideOutputLateData(lateTag)
            .process(new ProcessWindowFunction<Tuple2<String, Long>, Object, String, TimeWindow>() {
                @Override
                public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Object, String, TimeWindow>.Context context,
                                    Iterable<Tuple2<String, Long>> input, Collector<Object> collector) throws Exception {
                    long count = 0;
                    ArrayList<String> arr = new ArrayList<>();
                    for(Tuple2<String, Long> in: input){
                        arr.add("  [" + in.f0 + ", " + sdf.format(in.f1) +"]  ");
                        count++;
                    }
                    collector.collect("Window: " + sdf.format(context.window().getStart())+ " - "
                            + sdf.format(context.window().getEnd())
                            +" \n\tcount: " + count
                            + " \n\twatermark: " + sdf.format(context.currentWatermark())
                            + " \n\tData: " + arr
                    );
                }
            });


        streamOut.getSideOutput(lateTag).print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> side output:");


        env.execute("My WaterMark Test");
    }
}
