package com.will.tutorial.join;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream.IntervalJoined;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction.Context;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		WatermarkStrategy<WaterSensor> wsStrategy = WatermarkStrategy
				.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(10))
				.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
					@Override
					public long extractTimestamp(WaterSensor element, long recordTimestamp) {
						return element.getTs();
					}
				});

		KeyedStream<WaterSensor, String> ks1 = env
				.socketTextStream("localhost", 2121)
				.map(new WaterSensorMapFunction())
				.assignTimestampsAndWatermarks(wsStrategy)
				.keyBy(ws -> ws.getId());

		ks1.map(ws->ws.getTs()).print("=====L:");

		KeyedStream<WaterSensor, String> ks2 = env
				.socketTextStream("localhost", 2122)
				.map(new WaterSensorMapFunction())
				.assignTimestampsAndWatermarks(wsStrategy)
				.keyBy(ws -> ws.getId());

		ks2.map(ws->ws.getTs()).print("#####R:");

		OutputTag<WaterSensor> lateData = new OutputTag<WaterSensor>("lateData", Types.POJO(WaterSensor.class));

		SingleOutputStreamOperator<String> outStream = ks1.intervalJoin(ks2)
				.between(Time.seconds(0), Time.seconds(10))
				.sideOutputLeftLateData(lateData)
				.process(
						new ProcessJoinFunction<WaterSensor, WaterSensor, String>() {
							@Override
							public void processElement(
									WaterSensor left,
									WaterSensor right,
									ProcessJoinFunction<WaterSensor, WaterSensor, String>.Context ctx,
									Collector<String> out) throws Exception {
								out.collect("                                        L:" + ctx.getLeftTimestamp()
										+ ", R:" + ctx.getRightTimestamp()
										+ " -> JOIN:" + ctx.getTimestamp() + "                                        ");
							}
						}
				);

		outStream.print();

		outStream.getSideOutput(lateData).printToErr("@@@@LATE:");

		env.execute();
	}
}
