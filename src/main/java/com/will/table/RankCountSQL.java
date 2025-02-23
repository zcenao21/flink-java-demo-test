package com.will.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class RankCountSQL {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> source = env
                .socketTextStream("localhost",9999);
        DataStream<Order> orders = source
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] split = value.split(",");
                        Order order = new Order(
                                split[0],
                                Long.parseLong(split[1]),
                                Long.parseLong(split[2]),
                                Long.parseLong(split[3])
                        );

                        return order;
                    }
                });


        source.print("input: ");

        tEnv.registerDataStream("will_order", orders);
        tEnv
            .executeSql("\n" +
                    "select\n" +
                    "   type\n" +
                    "   ,count(distinct orderId) cnt\n" +
                    "from\n" +
                    "(\n" +
                    "    select\n" +
                    "        *\n" +
                    "    from\n" +
                    "    (\n" +
                    "        SELECT\n" +
                    "            orderId\n" +
                    "            ,price\n" +
                    "            ,type\n" +
                    "            ,utime\n" +
                    "            ,row_number() over (partition by orderId order by utime desc) as r\n" +
                    "        FROM will_order\n" +
                    "    ) a\n" +
                    "    where a.r<=1\n" +
                    ") b\n" +
                    "group by type\n" +
                    "\n")
            .print();

        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class Order {
        public String orderId;// 订单ID
        public long price; //价格
        public long type; //订单类型，1：自取 2：外卖
        public long utime;//更新时间

        // public constructor to make it a Flink POJO
        public Order() {}

        public Order(String orderId, long price, long type, long utime) {
            this.orderId = orderId;
            this.price = price;
            this.type=type;
            this.utime = utime;
        }

        @Override
        public String toString() {
            return "Order:" + orderId + " price:" + price+ " type:"+ type + " utime:" + utime;
        }
    }
}
