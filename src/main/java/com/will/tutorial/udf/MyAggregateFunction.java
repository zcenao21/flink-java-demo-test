package com.will.tutorial.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;

public class MyAggregateFunction {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

		DataStreamSource<Tuple2<Integer, Integer>> dsSource = env.fromElements(
				Tuple2.of(90, 10),
				Tuple2.of(80, 80),
				Tuple2.of(70, 10)
		);

		Table scoreWeightTable = stEnv.fromDataStream(dsSource, $("f0").as("score"), $("f1").as("weight"));
		stEnv.createTemporaryView("scoreTable", scoreWeightTable);

		stEnv.registerFunction("WeightAgg", new WeightAgg());

		stEnv.sqlQuery("select WeightAgg(score, weight) from scoreTable")
				.execute()
				.print();
	}

	// 泛型
	// 第一个代表返回值
	// 第二个表示累加器类型。参数类型，第一个代表加权求和，第二个代表权重求和
	public static class WeightAgg extends AggregateFunction<Double, Tuple2<Integer, Integer>>{

		@Override
		public Double getValue(Tuple2<Integer, Integer> accVal) {
			return accVal.f0*1.0/accVal.f1;
		}

		@Override
		public Tuple2<Integer, Integer> createAccumulator() {
			return new Tuple2<>(0,0);
		}

		public void accumulate(Tuple2<Integer, Integer> accVal, int score, int weight) {
			accVal.f0 += score*weight;
			accVal.f1 += weight;
		}
	}
}
