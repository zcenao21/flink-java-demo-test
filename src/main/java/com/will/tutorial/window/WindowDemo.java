package com.will.tutorial.window;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SingleOutputStreamOperator<WaterSensor> sensorDS =
				env.socketTextStream("localhost", 2121)
				.map(new WaterSensorMapFunction());
		sensorDS.keyBy(ws->ws.getId())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
					@Override
					public Integer createAccumulator() {
						System.out.println("createAccumulator方法调用");
						return 0;
					}

					@Override
					public Integer add(WaterSensor value, Integer accumulator) {
						System.out.println("add方法调用，value=" + value + "accumulator=" + accumulator);
						return value.getVc() + accumulator;
					}

					@Override
					public String getResult(Integer accumulator) {
						System.out.println("getResult方法调用，accumulator=" + accumulator);
						return accumulator+"";
					}

					@Override
					public Integer merge(Integer a, Integer b) {
						return null;
					}
				}).print("窗口输出");

		env.execute();
	}
}
