package com.will.stream.timer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;

public class SimulateKafkaSource implements SourceFunction<Stock> {
    boolean is_running = true;

    Random rand = new Random();

    @Override
    public void run(SourceContext<Stock> ctx) throws Exception {
        long i = 1;
        while (is_running) {
            long stockPrice = 0;
            //生成水印
            stockPrice = rand.nextInt(20)+i++%20;
            Stock element = new Stock("will", stockPrice, System.currentTimeMillis());
            long eventTime = System.currentTimeMillis();
            ctx.collectWithTimestamp(element, eventTime);
            ctx.emitWatermark(new Watermark(eventTime));
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
