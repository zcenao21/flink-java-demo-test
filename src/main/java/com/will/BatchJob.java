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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 *
 * 使用了DataSet API，实例程序
 *
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final Pattern pattern= Pattern.compile("\\d+.*\\d*");


		DataSet<String> input = env.readTextFile("/home/will/tmpdir/input.txt")
				.name("Word Count Reading From File");
		
		DataSet<Tuple2<String,Integer>> out = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] ss = s.split("[,:\\s+()]");
				for(String in: ss){
					collector.collect(new Tuple2<>(in,1));
				}
			}
		});

		out.groupBy(0)
			.sum(1)
			.filter(new FilterFunction<Tuple2<String, Integer>>() {
				@Override
				public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
					return pattern.matcher(stringIntegerTuple2.getField(0)).matches()
							||stringIntegerTuple2.getField(0).toString().length()<1?false:true;
				}
			})
			.first(3)
			.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> si1, Tuple2<String, Integer> si2) throws Exception {
					return new Tuple2<>(si1.getField(0)+si2.getField(0).toString(),(int)si1.getField(1)+(int)si2.getField(1));
				}
			})
			.print();

		// execute program
//		env.execute("Flink Batch Java API Skeleton");
	}
}
