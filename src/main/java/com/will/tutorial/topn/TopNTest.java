package com.will.tutorial.topn;

import com.will.tutorial.aggregate.pojo.WaterSensor;
import com.will.tutorial.map.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopNTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<String> ds = env.socketTextStream("localhost", 2121);
		SingleOutputStreamOperator<WaterSensor> wsDs =
				ds
					.map(new WaterSensorMapFunction())
					.assignTimestampsAndWatermarks(
							WatermarkStrategy
							.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
							.withTimestampAssigner((ws, ts)->ws.getTs()*1000L));

		SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggResult = wsDs
				.keyBy(ws -> ws.getVc())
				.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
				.aggregate(new AggregateSensor(),
						new ProcessSensor()
				);

		aggResult.keyBy(t->t.f2).process(new TopN(3)).print();
		env.execute();
	}

	public static class AggregateSensor implements AggregateFunction<WaterSensor, Integer, Integer>{
			@Override
			public Integer createAccumulator() {
				return 0;
			}

			@Override
			public Integer add(WaterSensor value, Integer accumulator) {
				return accumulator + 1;
			}

			@Override
			public Integer getResult(Integer accumulator) {
				return accumulator;
			}

			@Override
			public Integer merge(Integer a, Integer b) {
				return null;
			}
	}

	public static class ProcessSensor extends ProcessWindowFunction<Integer, Tuple3<Integer,Integer,Long>, Integer, TimeWindow> {
		@Override
		public void process(Integer key
				, ProcessWindowFunction<Integer, Tuple3<Integer,Integer,Long>, Integer, TimeWindow>.Context context
				, Iterable<Integer> elements
				, Collector<Tuple3<Integer,Integer,Long>> out) throws Exception {
			String startTime = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
			String endTime = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");

			int acc = elements.iterator().next();
			//System.out.println("current Key: " + key + " window:[" + startTime + "," + endTime+")" + " value:" + acc);
			out.collect(new Tuple3<>(key, acc, context.window().getEnd()));
		}
	}

	public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
		Integer N; // 取top几

		// 存储某个时间窗口的数据
		// key为时间窗口结束
		// v为时间窗口内所有水位出现次数的统计值列表
		Map<Long, List<Tuple3<Integer, Integer, Long>>> res = new HashMap();
		TopN(int N){
			this.N = N;
		}

		@Override
		public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
			super.onTimer(timestamp, ctx, out);
			Long windowEnd = ctx.getCurrentKey();

			List<Tuple3<Integer, Integer, Long>> vcCountList = res.get(windowEnd);
			vcCountList.sort((o1, o2) -> o2.f1-o1.f1);

			StringBuilder sb = new StringBuilder();
			sb.append("==============start================\n");
			sb.append("时间范围[" + (vcCountList.get(0).f2/1000-10) + "-" + vcCountList.get(0).f2/1000 + ")\n");
			for(int i=0;i<Math.min(N,vcCountList.size());i++){
				sb.append("Top " + (i+1) + " 水位为"+vcCountList.get(i).f0+"，出现了"+vcCountList.get(i).f1 + "次\n");
			}
			sb.append("==============end================\n");

			res.remove(windowEnd);
			out.collect(sb.toString());
		}

		@Override
		public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
			// key, 计数值， 窗口标识
			if(res.containsKey(value.f2)){
				List<Tuple3<Integer, Integer, Long>> vcCount = res.get(value.f2);
				vcCount.add(value);
			}else{
				List<Tuple3<Integer, Integer, Long>> vcCount = new ArrayList<>();
				vcCount.add(value);
				res.put(value.f2, vcCount);
			}

			// 设置触发时间为窗口结束时间+1ms
			ctx.timerService().registerEventTimeTimer(value.f2+1L);
		}
	}
}









