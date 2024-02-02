package com.will.tutorial.map;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
	@Override
	public WaterSensor map(String line) throws Exception {
		String[] parts = line.split(",");
		return new WaterSensor(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
	}
}
