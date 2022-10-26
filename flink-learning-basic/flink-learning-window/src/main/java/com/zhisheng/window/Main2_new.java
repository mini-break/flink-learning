package com.zhisheng.window;

import com.zhisheng.common.model.WordEvent;
import com.zhisheng.function.CustomSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Desc: Flink Window & Watermark
 * Created by zhisheng on 2019-05-14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main2_new {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        // DataStream<WordEvent> data = env.addSource(new CustomSource())
        //         .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WordEvent>() {
        //             private long currentTimestamp = Long.MIN_VALUE;
        //
        //             private final long maxTimeLag = 5000;
        //
        //             @Nullable
        //             @Override
        //             public Watermark getCurrentWatermark() {
        //                 return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
        //             }
        //
        //             @Override
        //             public long extractTimestamp(WordEvent element, long previousElementTimestamp) {
        //                 long timestamp = element.getTimestamp();
        //                 currentTimestamp = Math.max(timestamp, currentTimestamp);
        //                 return timestamp;
        //             }
        //         });

        final long[] currentTimestamp = {Long.MIN_VALUE};
        // 延迟值 5s
        final long maxTimeLag = 5000;
        DataStream<WordEvent> data = env.addSource(new CustomSource()) // 加入自定义数据源
                .assignTimestampsAndWatermarks((new WatermarkStrategy<WordEvent>() { // 增加水位线
                    @Override
                    public WatermarkGenerator<WordEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<WordEvent>() {
                            /**
                             * onEvent方法在接收到每一个事件数据时就会触发调用，
                             * 第一个参数event为接收的事件数据，
                             * 第二个参数eventTimestamp表示事件时间戳，
                             * 第三个参数output可用output.emitWatermark方法生成一个Watermark.
                             */
                            @Override
                            public void onEvent(WordEvent event, long eventTimestamp, WatermarkOutput output) {

                            }

                            /**
                             * onPeriodicEmit方法会周期性触发（周期性：可以调用环境配置 env.getConfig().setAutoWatermarkInterval(xxx)方法来设置，默认为200ms），
                             * 比每个元素生成一个Watermark效率高。
                             * 接收一个WatermarkOutput类型的参数output，内部可用output.emitWatermark方法生成一个Watermark
                             */
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                long timestamp = System.currentTimeMillis() - maxTimeLag;
                                System.out.println("水位值：" + timestamp);
                                output.emitWatermark(new Watermark(currentTimestamp[0] == Long.MIN_VALUE ? Long.MIN_VALUE : timestamp));
                            }
                        };
                    }
                }).withTimestampAssigner(new SerializableTimestampAssigner<WordEvent>() {
                    @Override
                    public long extractTimestamp(WordEvent element, long recordTimestamp) {
                        long timestamp = element.getTimestamp();
                        currentTimestamp[0] = Math.max(timestamp, currentTimestamp[0]);
                        return timestamp;
                    }
                }));
//        data.print();
        data.keyBy(WordEvent::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))// 滚动窗口为10s
                .apply(new WindowFunction<WordEvent, WordEvent, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WordEvent> input, Collector<WordEvent> out) throws Exception {
                        System.out.println(window.getStart() + " " + window.getEnd() + " w");
                        for (WordEvent word : input) {
                            out.collect(word);
                        }
                    }
                })
//                .sum("count")
                .print();

        env.execute("zhisheng —— flink window example");
    }
}
