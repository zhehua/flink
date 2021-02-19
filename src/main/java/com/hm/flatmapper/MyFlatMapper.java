package com.hm.flatmapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
    /**
     *
     * @param s 输入读取的文本的一行
     * @param collector 收集结果
     * @throws Exception
     */
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String words[]=s.split(" ");//逐行读取，按空格分割
        Arrays.stream(words).forEach(word->collector.collect(new Tuple2<>(word,1)));
    }
}
