package com.will.stream.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class WatermarkTest {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 设置自动产生输入，并设置输入格式
        DataStream<String> source = env
                .socketTextStream("localhost",9999);


        // 解析输入，三列分别表示输入字符串，事件时间，水印时间
        DataStream<Tuple3<String, Long, Long>> stream = source
                .flatMap(new FlatMapFunction<String, Tuple3<String, Long, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
                        String[] ss = s.split(",");
                        Tuple3<String, Long, Long> input = new Tuple3<String, Long, Long>();
                        try{
                            input.f0 = ss[0];
                            input.f1 = sdf.parse(ss[1]).getTime();
                            input.f2 = sdf.parse(ss[2]).getTime();
                        }catch (Exception e){
                            System.out.println("cant not parse " + input);
                            input.f0 = "";
                            input.f1= 0L;
                            input.f2 = 0L;
                        }
                        collector.collect(input);
                    }
                });

        stream.map((MapFunction<Tuple3<String, Long, Long>, Object>) input -> new Tuple3<>(input.f0, sdf.format(input.f1), sdf.format(input.f2)))
            .print("input ");

        stream
            .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple3<String, Long, Long>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public WatermarkGenerator<Tuple3<String, Long, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                    return new WatermarkGenerator<Tuple3<String, Long, Long>>() {
                        private static final long serialVersionUID = 1L;
                        long myWatermark;
                        @Override
                        public void onEvent(Tuple3<String, Long, Long> stringLongLongTuple3, long l, WatermarkOutput watermarkOutput) {
                            myWatermark = stringLongLongTuple3.f2;
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                            watermarkOutput.emitWatermark(new Watermark(myWatermark));
                        }
                    };
                }

                @Override
                public TimestampAssigner<Tuple3<String, Long, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                    return new TimestampAssigner() {
                        @Override
                        public long extractTimestamp(Object o, long l) {
                            Tuple3<String, Long, Long> input = (Tuple3<String, Long, Long>)(o);
                            return input.f2;
                        }
                    };
                }
            })
            .setParallelism(1)
            .keyBy(event->event.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new ProcessWindowFunction<Tuple3<String, Long, Long>, Object, String, TimeWindow>() {
                @Override
                public void process(String s, ProcessWindowFunction<Tuple3<String, Long, Long>, Object, String, TimeWindow>.Context context,
                                    Iterable<Tuple3<String, Long, Long>> input, Collector<Object> collector) throws Exception {
                    long count = 0;
                    ArrayList<String> arr = new ArrayList<>();
                    for(Tuple3<String, Long, Long> in: input){
                        arr.add("  [" + in.f0 + ", " + sdf.format(in.f1) + "," + sdf.format(in.f2) +"]  ");
                        count++;
                    }
                    collector.collect("Window: " + sdf.format(context.window().getStart())+ " - "
                            + sdf.format(context.window().getEnd())
                            +" count: " + count + " \n\t\tData: " + arr);
                }
            })
            .print();

        env.execute("My WaterMark Test");
    }
}
