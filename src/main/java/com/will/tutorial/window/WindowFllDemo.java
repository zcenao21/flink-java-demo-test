package com.will.tutorial.window;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFllDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<WaterSensor> sensorDS =
				env.socketTextStream("localhost", 2121)
				.map(new WaterSensorMapFunction());
		SingleOutputStreamOperator<String> out = sensorDS.keyBy(ws -> ws.getId())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
					@Override
					public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
						String outS = "";
						for (WaterSensor sIn : elements) {
							outS += sIn.getId() + "-" + sIn.getVc() + "    ";
						}
						System.out.println("current key:" + s + " elements:" + outS);
						out.collect(outS);
					}
				});
		out.print("out");
		env.execute();
	}
}
