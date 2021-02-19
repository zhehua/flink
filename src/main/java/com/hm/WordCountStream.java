package com.hm;

import com.hm.flatmapper.MyFlatMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        //获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStream<String> inputDateStream = env.readTextFile("D:\\Download\\micro-service\\wordcount\\src\\main\\resources\\word.txt");
        //数据统计，采用二元组(word,1)格式保存，类似hashmap
        DataStream<Tuple2<String, Integer>> sum = inputDateStream.flatMap(new MyFlatMapper()).keyBy(0)//按二元组0位分组
                .sum(1);//对二元组1位求和
        sum.print();//控制台输出
        //跟批处理不一样，流处理的话，以上只是定义，还需要执行
        env.execute();
    }
}
