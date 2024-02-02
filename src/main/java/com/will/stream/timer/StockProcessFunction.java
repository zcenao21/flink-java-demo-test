package com.will.stream.timer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class StockProcessFunction extends KeyedProcessFunction<String, Stock, String> {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Long timerDelay = 5000L;
    private LinkedList<Long> priceList = new LinkedList<>();
    private ValueState<Long> lastPriceState;
    private ValueState<Long> timestampState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastPriceState = getRuntimeContext().getState(
                new ValueStateDescriptor<Long>("lastPrice", Types.LONG));
        timestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<Long>("timestamp", Types.LONG)
        );
    }

    @Override
    public void processElement(Stock stock, KeyedProcessFunction<String, Stock,
            String>.Context context, Collector<String> collector) throws Exception {
        if(null == lastPriceState.value()){
            // 不处理,首次无价格输入
        }else{
            priceList.add(stock.stockPrice);
            Long lPrice = lastPriceState.value();
            long currTimestamp;
            if(null == timestampState.value()){
                currTimestamp = 0L;
            }else{
                currTimestamp = timestampState.value();
            }

            // 当价格降低时清除timer
            if(stock.getStockPrice()<lPrice){
                context.timerService().deleteEventTimeTimer(currTimestamp);
                System.out.println(formatter.format(System.currentTimeMillis())+
                        " ========================================================= clear    timer:" +
                        formatter.format(currTimestamp)+
                        " "+stock.getStockPrice()+" "+lPrice);
                timestampState.clear();
            }else{
                Long delayedTimestamp = context.timestamp()+timerDelay;
                System.out.println(formatter.format(System.currentTimeMillis())+
                        " ========================================================= register timer:" +
                        formatter.format(delayedTimestamp));
                context.timerService().registerEventTimeTimer(delayedTimestamp);
                timestampState.update(delayedTimestamp);
            }
        }
        lastPriceState.update(stock.stockPrice);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Stock,
            String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        List<Long> rest = (List<Long>)priceList.clone();
        priceList.clear();

        System.out.println(formatter.format(System.currentTimeMillis())+
                " ========================================================= on      timer:" +formatter.format(timestamp)+"-"+
                rest);
        timestampState.clear();
    }
}
