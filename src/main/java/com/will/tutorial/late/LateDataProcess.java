package com.will.tutorial.late;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class LateDataProcess {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		SingleOutputStreamOperator<WaterSensor> waterSensorDs =
				env
						.socketTextStream("127.0.0.1", 2121)
						.map(new WaterSensorMapFunction());

//		waterSensorDs.print();

		KeyedStream<WaterSensor, String> wordGroup = waterSensorDs.map(r -> {
			int ts = r.getTs() * 1000;
			return new WaterSensor(r.getId(), ts, r.getVc());
		}).assignTimestampsAndWatermarks(
				WatermarkStrategy
						.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((r, t) -> r.getTs())
		).keyBy(r -> r.getId());

		SingleOutputStreamOperator<String> process = wordGroup.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.allowedLateness(Time.seconds(10))
				.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
					@Override
					public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
						StringBuilder sb = new StringBuilder();
						for (WaterSensor e : elements) {
							sb.append(e.getId() + " " + e.getTs()/1000 + " | ");
						}
						System.out.println("window:["+context.window().getStart()/1000+", " +context.window().getEnd()/1000+"]");
						out.collect(sb.toString());
					}
				});
//				.reduce(new ReduceFunction<WaterSensor>() {
//					@Override
//					public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//						value1.setVc(Math.max(value1.getVc(), value2.getVc()));
//						return value1;
//					}
//				});

		process.print(">>");

		env.execute("late data process");

	}
}
