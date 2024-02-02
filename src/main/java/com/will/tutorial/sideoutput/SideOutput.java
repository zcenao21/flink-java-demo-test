package com.will.tutorial.sideoutput;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutput {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

		OutputTag<Integer> fp = new OutputTag<>("fp", Types.INT);
		OutputTag<Integer> sp = new OutputTag<>("sp", Types.INT);

		SingleOutputStreamOperator<Integer> mp = source.process(new ProcessFunction<Integer, Integer>() {
			@Override
			public void processElement(Integer input, ProcessFunction<Integer, Integer>.Context context, Collector<Integer> collector) throws Exception {
				if (input % 3 == 1) {
					context.output(fp, input);
				} else if (input % 3 == 2) {
					context.output(sp, input);
				} else {
					collector.collect(input);
				}
			}
		});

		mp.print("主流:");
		mp.getSideOutput(fp).print("fp:");
		mp.getSideOutput(sp).print("sp:");

		env.execute();
	}
}
