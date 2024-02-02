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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 使用方法：
 * 1.nc -lk 9999  开启连接
 * 2.运行程序
 * 3.观察程序输出
 */
public class WindowAllApplyTest {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		// final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

		DataStream<String> source = env
				.socketTextStream("localhost",9999);

		source
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
			.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
			.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, TimeWindow>() {
				@Override
				public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String,Integer>> collector) throws Exception {
					Tuple2<String,Integer> sum = new Tuple2<>("",0);
					for(Tuple2<String,Integer> t:iterable){
						sum.setField(sum.f0+t.f0,0);
						sum.setField(t.f1+(Integer) sum.f1,1);
					}
					collector.collect(new Tuple2<String,Integer>(sum.f0,sum.f1));
				}
			}).print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
