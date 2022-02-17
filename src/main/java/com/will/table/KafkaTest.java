package com.will.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "testGroup");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink-sink", new SimpleStringSchema(), properties));
        stream.print();
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
