//package com.will.tutorial.timer;
//
//import com.will.tutorial.aggregate.pojo.WaterSensor;
//import com.will.tutorial.map.WaterSensorMapFunction;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
//import org.apache.flink.util.Collector;
//
//public class WordCount {
//	public static void main(String[] args) throws Exception {
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(1);
//
//		SingleOutputStreamOperator<WaterSensor> waterSensorDs =
//				env
//						.socketTextStream("localhost", 2121)
//						.map(new WaterSensorMapFunction());
//
//		waterSensorDs.keyBy(ws->ws.getId())
//						.process(new KeyedProcessFunction<String, WaterSensor, String>() {
//							@Override
//							public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
//								System.out.println("key:"+ctx.getCurrentKey()+" value:" + value);
//								ctx.timerService().registerProcessingTimeTimer(10);
//							}
//
//							@Override
//							public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//								super.onTimer(timestamp, ctx, out);
//
//							}
//						})
//
//		waterSensorDs.print();
//
//		env.execute();
//	}
//}
