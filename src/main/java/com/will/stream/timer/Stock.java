package com.will.stream.timer;

import lombok.Data;

import java.text.SimpleDateFormat;

@Data
public class Stock {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String stockName;
    Long stockPrice;

    Long time;

    public Stock(String name, long price, long time) {
        this.stockName = name;
        this.stockPrice = price;
        this.time = time;
    }

    @Override
    public String toString(){
        return "Stock(name="+stockName+", price="+stockPrice+", time="+formatter.format(time)+")";
    }
}
