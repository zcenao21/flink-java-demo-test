package com.will.tutorial.connect;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.Context;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.ReadOnlyContext;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectBroadCastDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		SingleOutputStreamOperator<WaterSensor> waterSensorDs =
				env
						.socketTextStream("localhost", 2121)
						.map(new WaterSensorMapFunction());

		DataStreamSource<String> dsStr = env
				.socketTextStream("localhost", 2122);

		MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>("bs", String.class, Integer.class);
		BroadcastStream<String> broadcastDS = dsStr.broadcast(mapStateDescriptor);

		SingleOutputStreamOperator<String> processDS = waterSensorDs.connect(broadcastDS).process(new BroadcastProcessFunction<WaterSensor, String, String>() {
			@Override
			public void processElement(WaterSensor ws, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
				ReadOnlyBroadcastState<String, Integer> thresholdState = ctx.getBroadcastState(mapStateDescriptor);
				Integer threshold = thresholdState.get("threshold");
				threshold = threshold == null ? 0:threshold;
				if(ws.getVc()>threshold){
					System.out.println("the water sensor value is " + ws.getVc() + ", which is greater than  threshold " + threshold);
				}
			}

			@Override
			public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
				BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
				broadcastState.put("threshold", Integer.parseInt(value));
				System.out.println("threshold taking effect now! value: " + value);
			}
		});

		env.execute();
	}
}
