package com.will.tutorial.connect;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.Context;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Tuple2<Integer, String>> s1 = env.fromElements(
				Tuple2.of(1, "f1"),
				Tuple2.of(1, "f2"),
				Tuple2.of(2, "f3"),
				Tuple2.of(3, "f4")
		);
		DataStreamSource<Tuple2<Integer, String>> s2 = env.fromElements(
				Tuple2.of(1, "s1"),
				Tuple2.of(1, "s2"),
				Tuple2.of(2, "s3"),
				Tuple2.of(3, "s4"),
				Tuple2.of(4, "s5")
		);

		s1.connect(s2).keyBy(s -> s.f0, s -> s.f0)
				.process(
						new CoProcessFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>() {
							Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap();
							Map<Integer, List<Tuple2<Integer, String>>> s2Cache = new HashMap();

							@Override
							public void processElement1(Tuple2<Integer, String> value,
														CoProcessFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>.Context ctx,
														Collector<Tuple2<Integer, String>> out) throws Exception {
								System.out.println("process element 1:" + value);
								if (s1Cache.containsKey(value.f0)) {
									s1Cache.get(value.f0).add(value);
								} else {
									ArrayList<Tuple2<Integer, String>> tuple2s = new ArrayList<>();
									tuple2s.add(value);
									s1Cache.put(value.f0, tuple2s);
								}

								if (s2Cache.containsKey(value.f0)) {
									for (Tuple2<Integer, String> s2Ele : s2Cache.get(value.f0)) {
										System.out.println("====================> f1 input:" + value + " s2 match:" + s2Ele);
									}
								}

								out.collect(value);
							}

							@Override
							public void processElement2(Tuple2<Integer, String> value,
														CoProcessFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>.Context ctx,
														Collector<Tuple2<Integer, String>> out) throws Exception {
								System.out.println("process element 1:" + value);
								if (s2Cache.containsKey(value.f0)) {
									s2Cache.get(value.f0).add(value);
								} else {
									ArrayList<Tuple2<Integer, String>> tuple2s = new ArrayList<>();
									tuple2s.add(value);
									s2Cache.put(value.f0, tuple2s);
								}

								if (s1Cache.containsKey(value.f0)) {
									for (Tuple2<Integer, String> s1Ele : s1Cache.get(value.f0)) {
										System.out.println("====================> s2 input:" + value + " s1 match:" + s1Ele);
									}
								}

								out.collect(value);
							}
						});

		env.execute();

	}
}
