package com.will.tutorial.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGen {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);
		DataGeneratorSource<String> genSource = new DataGeneratorSource<>(
			new GeneratorFunction<Long, String>() {
				@Override
				public String map(Long aLong) throws Exception {
					return "val:" + aLong.toString();
				}
			}
			, 100
			, RateLimiterStrategy.perSecond(1)
			, Types.STRING
		);
		env.fromSource(genSource, WatermarkStrategy.noWatermarks(), "test")
				.print();
		env.execute();

	}
}
