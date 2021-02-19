package com.hm;

import com.hm.flatmapper.MyFlatMapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //读取数据
        DataSource<String> inputDateSource = env.readTextFile("D:\\Download\\micro-service\\flink\\src\\main\\resources\\word.txt");
        //数据统计，采用二元组(word,1)格式保存，类似hashmap
        DataSet<Tuple2<String, Integer>> sum = inputDateSource.flatMap(new MyFlatMapper()).groupBy(0)//按二元组0位分组
                .sum(1);//对二元组1位求和
        sum.print();//控制台输出
    }
}
