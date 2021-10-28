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

package com.will;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 学习使用Iteration功能
 */
public class StreamingJobIteration {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStream<Long> source = env.generateSequence(0,1000);

		IterativeStream<Long> ites = source.iterate();

		DataStream<Long> minus1 = ites
				.map(new MapFunction<Long, Long>() {
					@Override
					public Long map(Long aLong) throws Exception {
						return aLong-1;
					}
				});

		DataStream<Long> stillGreaterThanZero = minus1
				.filter(new FilterFunction<Long>() {
					@Override
					public boolean filter(Long aLong) throws Exception {
						return aLong>0;
					}
				});

		ites.closeWith(stillGreaterThanZero);

		DataStream<Long> lessThanZero = minus1
				.filter(new FilterFunction<Long>() {
					@Override
					public boolean filter(Long aLong) throws Exception {
						return aLong<=0;
					}
				});

		lessThanZero.print();

		// execute program
		env.execute();
	}
}
