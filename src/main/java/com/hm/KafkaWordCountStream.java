package com.hm;

import com.hm.flatmapper.MyFlatMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaWordCountStream {
    public static void main(String[] args) throws Exception {
        //获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置
        Properties properties =new Properties();
        properties.put("bootstrap.servers", "192.168.1.127:9092");
        properties.put("group.id","consumer-group" );
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        //读取数据
        DataStream<String> inputDateStream = env.addSource(new FlinkKafkaConsumer011<String>("topic",new SimpleStringSchema(),properties));
        inputDateStream.print();//控制台输出
        //跟批处理不一样，流处理的话，以上只是定义，还需要执行
        env.execute();
    }
}
