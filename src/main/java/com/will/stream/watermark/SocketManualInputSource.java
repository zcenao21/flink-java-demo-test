package com.will.stream.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class SocketManualInputSource implements SourceFunction<Tuple2<String, Long>> {
    boolean is_running = true;
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        long i = 1;
        while (is_running) {

        }
    }

    @Override
    public void cancel() {
        is_running = false;
    }
}
