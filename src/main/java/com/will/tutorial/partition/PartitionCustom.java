package com.will.tutorial.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustom {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		DataStreamSource<String> source = env.socketTextStream("localhost", 2121);
		source.partitionCustom(new Partitioner<String>() {
			@Override
			public int partition(String s, int i) {
				return Integer.parseInt(s) % i;
			}
		}, w -> w).print();
		env.execute();
	}
}
