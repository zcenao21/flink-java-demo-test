package com.will.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class SinkTest {
    public static void main(String[] args) throws Exception {

        String brokers="localhost:9092";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> source = env
                .socketTextStream("localhost",9999);
        DataStream<WC> input = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] ss = s.split("[,:\\s+()]");
                        for(String in: ss){
                            collector.collect(new Tuple2<>(in,1));
                        }
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, WC>() {
                    @Override
                    public WC map(Tuple2<String, Integer> t2) throws Exception {
                        return new WC(t2.f0,t2.f1,System.currentTimeMillis());
                    }
                });

        tEnv.registerDataStream("WordCount", input);
        Table table = tEnv.sqlQuery("SELECT word,frequency,timein frequency FROM WordCount where CHAR_LENGTH(word)>1");
        DataStream<String> result = tEnv.toDataStream(table, WC.class).map(new MapFunction<WC, String>() {
            @Override
            public String map(WC wc) throws Exception {
                return wc.toString();
            }
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "flink-sink",                  // 目标 topic
                new SimpleStringSchema(),
                properties);                  // producer 配置); // 容错

        result.addSink(myProducer);
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class WC {
        public String word;//hello
        public long frequency;//1
        public long timein;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency, long timein) {
            this.word = word;
            this.frequency = frequency;
            this.timein=timein;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency+" "+timein;
        }
    }
}
