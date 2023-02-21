package com.will.stream.watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SimulateKafkaSource implements SourceFunction<Tuple2<String, Long>> {
    boolean is_running = true;

    Random rand = new Random();

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        long i = 1;
        while (is_running) {
            long eventTime = 1672502400000L+i*1000;
            //生成水印
            eventTime = eventTime-rand.nextInt(20)*1000;
            Tuple2<String, Long> element = new Tuple2<>("topic_1", eventTime);
            ctx.collectWithTimestamp(element, eventTime);
            i++;
            //每1秒一个数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        is_running = false;
    }
}
