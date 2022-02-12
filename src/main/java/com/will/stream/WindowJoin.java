/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.will.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用方法：
 * 1.nc -lk 9998  开启连接1
 * 1.nc -lk 9999  开启连接2
 * 2.运行程序
 * 3.观察程序输出
 */
public class WindowJoin {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> source1 = env
				.socketTextStream("localhost",9998);
		DataStream<String> source2 = env
				.socketTextStream("localhost",9999);

		DataStream<Tuple2<String,Integer>> result1 = source1
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
						String[] ss = s.split("[,:\\s+()]");
						for(String in: ss){
							collector.collect(new Tuple2<>(in,1));
						}
					}
				})
			.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
			.sum(1);

		DataStream<Tuple2<String,Integer>> result2 = source2
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
						String[] ss = s.split("[,:\\s+()]");
						for(String in: ss){
							collector.collect(new Tuple2<>(in,1));
						}
					}
				})
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.sum(1);

		result1.join(result2)
				.where(v->v.f0).equalTo(v->v.f0)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(20),Time.seconds(5)))
				.apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
						   @Override
						   public Tuple2<String, Integer> join(Tuple2<String, Integer> st1, Tuple2<String, Integer> st2) throws Exception {
							   return new Tuple2<>(st1.f0, st1.f1 + st2.f1);
						   }
					   }
				)
				.print();

		// execute program
		env.execute("Flink Streaming Java API Skebleton");
	}
}
