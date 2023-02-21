package com.will.stream.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

public class MyWatermark implements WatermarkGenerator<Tuple2<String, Long>> {
    private long maxTimeStamp;

    private long delay = 5000;

    Random rand = new Random();

    @Override
    public void onEvent(Tuple2<String, Long> input, long eventTime, WatermarkOutput watermarkOutput) {
        maxTimeStamp = eventTime;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(maxTimeStamp +delay - rand.nextInt(20)*1000));
    }
}
