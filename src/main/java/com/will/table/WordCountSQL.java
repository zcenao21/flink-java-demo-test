package com.will.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
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
                .window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(1)))
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, WC>() {
                    @Override
                    public WC map(Tuple2<String, Integer> t2) throws Exception {
                        return new WC(t2.f0,t2.f1,System.currentTimeMillis());
                    }
                });

        tEnv.registerDataStream("WordCount", input, "word, frequency,timein");
        Table table = tEnv.sqlQuery("SELECT word,frequency,timein frequency FROM WordCount where CHAR_LENGTH(word)>1");
        DataStream<WC> result = tEnv.toDataStream(table, WC.class);
        result.print();

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
