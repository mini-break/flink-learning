package com.zhisheng.window;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.function.LineSplitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.zhisheng.constant.WindowConstant.HOST_NAME;
import static com.zhisheng.constant.WindowConstant.PORT;

/**
 * Desc: Flink Window 学习
 * 操作：在终端执行 nc -lp 9000 ，然后输入 long text 类型的数据(例：1 abc)
 * Created by zhisheng on 2019-05-14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        DataStreamSource<String> data = env.socketTextStream(parameterTool.get(HOST_NAME), parameterTool.getInt(PORT));

        System.out.println(parameterTool.get(HOST_NAME) + ":" + parameterTool.get(PORT));
        //基于滚动时间窗口
        // data.flatMap(new LineSplitter())
        //         // .keyBy(1) // 废弃
        //         .keyBy(t -> t.f1)
        //         // .timeWindow(Time.seconds(30)) // 废弃
        //         .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
        //         .sum(0)
        //         .print();

        //基于滑动时间窗口
        // data.flatMap(new LineSplitter())
        //         .keyBy(t -> t.f1)
        //         .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(30)))
        //         .sum(0)
        //         .print();


        //基于事件数量滚动窗口
        // data.flatMap(new LineSplitter())
        //         .keyBy(t -> t.f1)
        //         .countWindow(3) // count(t.f1)=3 时输出，注意是count，不是sum。输出值为 sum(所有相同t.f1)
        //         .sum(0)
        //         .print();


        //基于事件数量滑动窗口
        // data.flatMap(new LineSplitter())
        //         .keyBy(t -> t.f1)
        //         .countWindow(4, 2) // count(t.f1)=2 时输出。输出值为 sum(最近4个相同t.f1)
        //         .sum(0)
        //         .print();


        //基于会话时间窗口
        data.flatMap(new LineSplitter())
                .keyBy(t -> t.f1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))) //表示如果 10s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
                .sum(0)
                .print();

        env.execute("zhisheng —— flink window example");
    }
}
