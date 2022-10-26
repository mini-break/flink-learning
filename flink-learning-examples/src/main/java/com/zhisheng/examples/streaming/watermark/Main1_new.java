package com.zhisheng.examples.streaming.watermark;

import com.zhisheng.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.zhisheng.common.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * Desc: Periodic Watermark(周期性的生成水印，不会针对每条消息都生成)
 * Created by zhisheng on 2019-07-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main1_new {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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

        final long[] currentTimestamp = {Long.MIN_VALUE};
        //Periodic Watermark
        data.assignTimestampsAndWatermarks(((WatermarkStrategy<Word>) context -> new WatermarkGenerator<Word>() {

            /**
             * onEvent方法在接收到每一个事件数据时就会触发调用，
             * 第一个参数event为接收的事件数据，
             * 第二个参数eventTimestamp表示事件时间戳，
             * 第三个参数output可用output.emitWatermark方法生成一个Watermark.
             */
            @Override
            public void onEvent(Word event, long eventTimestamp, WatermarkOutput output) {

            }

            /**
             * onPeriodicEmit方法会周期性触发（周期性：可以调用环境配置 env.getConfig().setAutoWatermarkInterval(xxx)方法来设置，默认为200ms），
             * 比每个元素生成一个Watermark效率高。
             * 接收一个WatermarkOutput类型的参数output，内部可用output.emitWatermark方法生成一个Watermark
             */
            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                long maxTimeLag = 5000;
                long timestamp = currentTimestamp[0] == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp[0] - maxTimeLag;
                System.out.println(System.currentTimeMillis() + " 生成水印:" + timestamp);
                output.emitWatermark(new Watermark(timestamp));
            }
        }).withTimestampAssigner((SerializableTimestampAssigner<Word>) (word, recordTimestamp) -> {
            long timestamp = word.getTimestamp();
            currentTimestamp[0] = Math.max(timestamp, currentTimestamp[0]);
            System.out.format("event timestamp = %s, %s, CurrentWatermark = %s, %s", word.getTimestamp(),
                    DateUtil.format(word.getTimestamp(), YYYY_MM_DD_HH_MM_SS),
                    recordTimestamp,
                    DateUtil.format(recordTimestamp, YYYY_MM_DD_HH_MM_SS));
            System.out.println();
            return word.getTimestamp();
        }));

        data.print();
        env.execute("watermark demo");
    }
}
