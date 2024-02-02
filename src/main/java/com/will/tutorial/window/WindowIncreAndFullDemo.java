package com.will.tutorial.window;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Iterator;

public class WindowIncreAndFullDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<WaterSensor> sensorDS =
				env.socketTextStream("localhost", 2121)
				.map(new WaterSensorMapFunction())
						.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)));
		SingleOutputStreamOperator<String> out = sensorDS.keyBy(ws -> ws.getId())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
					@Override
					public Integer createAccumulator() {
						System.out.println("createAccumulator called");
						return 0;
					}

					@Override
					public Integer add(WaterSensor value, Integer accumulator) {
						System.out.println("add called! value:" + value + " acc:" + accumulator);
						return value.getVc() + accumulator;
					}

					@Override
					public String getResult(Integer accumulator) {
						System.out.println("getResult called! acc:" + accumulator);
						return accumulator.toString();
					}

					@Override
					public Integer merge(Integer a, Integer b) {
						return null;
					}
				}, new ProcessWindowFunction<String, String, String, TimeWindow>() {
					@Override
					public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
						String outS = "";
						Iterator<String> iterator = elements.iterator();
						while(iterator.hasNext()){
							outS += iterator.next() + " ";
							System.out.println("iterator+=" + outS);
						}
						System.out.println("current key:" + s + " elements:" + outS + " time:" + DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"));
						out.collect(outS);
					}
				});

		out.print("out");
		env.execute();
	}
}
