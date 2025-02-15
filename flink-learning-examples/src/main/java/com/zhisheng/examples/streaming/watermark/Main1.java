package com.zhisheng.examples.streaming.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: Periodic Watermark(周期性的生成水印，不会针对每条消息都生成)
 * Created by zhisheng on 2019-07-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);
        //并行度设置为 1
        env.setParallelism(1);
//        env.setParallelism(4);

        SingleOutputStreamOperator<Word> data = env.socketTextStream("10.20.2.86", 7777)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
                    }
                });

        //Periodic Watermark
        data.assignTimestampsAndWatermarks(new WordPeriodicWatermark());

        data.print();
        env.execute("watermark demo");
    }
}
