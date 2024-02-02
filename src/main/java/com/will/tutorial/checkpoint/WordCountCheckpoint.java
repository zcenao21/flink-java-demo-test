package com.will.tutorial.checkpoint;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountCheckpoint {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointInterval(10000);
		env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop100:9000/user/flink/checkpoint/");
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		SingleOutputStreamOperator<WaterSensor> waterSensorDs =
				env
						.socketTextStream("172.16.201.129", 2121)
						.map(new WaterSensorMapFunction());

		SingleOutputStreamOperator<WaterSensor> vcSum = waterSensorDs.keyBy(ws -> ws.getId()).sum("vc");
		vcSum.print();

		env.execute();
	}
}
