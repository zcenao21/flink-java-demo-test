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

package com.will.cep;

import com.will.table.SinkTest;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 使用方法：
 * 1.nc -lk 9999  开启连接
 * 2.运行程序
 * 3.观察程序输出
 */
public class CEPTest {

	public static void main(String[] args) throws Exception {
        String brokers="localhost:9092";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> source = env
                .socketTextStream("localhost",9999);

        DataStream<SinkTest.WC> input = source
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
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, SinkTest.WC>() {
                    @Override
                    public SinkTest.WC map(Tuple2<String, Integer> t2) throws Exception {
                        return new SinkTest.WC(t2.f0,t2.f1,System.currentTimeMillis());
                    }
                });

        Pattern<SinkTest.WC,?> pattern = Pattern.<SinkTest.WC>begin("start").where(
                new SimpleCondition<SinkTest.WC>() {
                    @Override
                    public boolean filter(SinkTest.WC wc) throws Exception {
                        return wc.word.length()==3;
                    }
                }
        );

        PatternStream<SinkTest.WC> patternStream = CEP.pattern(input,pattern);
        patternStream.process(
                new PatternProcessFunction<SinkTest.WC, String>() {
                    @Override
                    public void processMatch(Map<String, List<SinkTest.WC>> map, Context context, Collector<String> collector) throws Exception {
                        collector.collect(map.toString());
                    }
                }
        ).print();
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
