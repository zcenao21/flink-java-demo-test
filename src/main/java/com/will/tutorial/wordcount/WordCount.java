package com.will.tutorial.wordcount;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		SingleOutputStreamOperator<WaterSensor> waterSensorDs =
				env
						.socketTextStream("172.16.201.129", 2121)
						.map(new WaterSensorMapFunction());

		waterSensorDs.print();

		env.execute();
	}
}
