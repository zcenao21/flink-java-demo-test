package com.will.tutorial.aggregate;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateSum {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<WaterSensor> source = env.fromElements(
				new WaterSensor("s1", 1, 1),
				new WaterSensor("s1", 1, 1),
				new WaterSensor("s2", 2, 1),
				new WaterSensor("s3", 3, 1)
		);
		env.setParallelism(2);
		source.keyBy(s -> s.getId()).sum("vc").print();
		env.execute();
	}
}
