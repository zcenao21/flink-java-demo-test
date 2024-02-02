package com.will.tutorial.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		DataStreamSource<String> source = env.socketTextStream("localhost", 2121);
		source.map(line->line.toLowerCase()).global().print();

		env.execute();
	}
}
