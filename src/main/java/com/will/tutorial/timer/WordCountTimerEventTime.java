package com.will.tutorial.timer;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class WordCountTimerEventTime {
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		env.setParallelism(2);

		SingleOutputStreamOperator<WaterSensor> wsDS =
				env
						.socketTextStream("localhost", 2121)
						.map(new WaterSensorMapFunction())
						.assignTimestampsAndWatermarks(
								WatermarkStrategy
										.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(10))
										.withTimestampAssigner((element, recordTimestamp) -> element.getTs())
						);

		wsDS.keyBy(ws -> ws.getId())
				.process(new KeyedProcessFunction<String, WaterSensor, String>() {
					@Override
					public void processElement(WaterSensor value,
											   KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
											   Collector<String> out) throws Exception {
						int eventTimeTrigger = value.getTs() + 5000;
						out.collect("key:" + ctx.getCurrentKey()
								+ " value:" + value
								+ " register a timer at " + formatter.format(eventTimeTrigger)
								+ " with watermark " + ctx.timerService().currentWatermark());
						ctx.timerService().registerEventTimeTimer(eventTimeTrigger);
					}

					@Override
					public void onTimer(long timestamp,
										KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx,
										Collector<String> out) throws Exception {
						super.onTimer(timestamp, ctx, out);
						out.collect("key:"
								+ ctx.getCurrentKey()
								+ " timer started!" + formatter.format(ctx.timestamp())
								+ " \t 3.ctx.timerService().currentWatermark():"
								+ ctx.timerService().currentWatermark());
					}
				}).print();

		env.execute();
	}
}
