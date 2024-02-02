package com.will.tutorial.aggregate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class WaterSensor {
	public String id;
	public int ts; // 时间
	public int vc; // 水位
}
