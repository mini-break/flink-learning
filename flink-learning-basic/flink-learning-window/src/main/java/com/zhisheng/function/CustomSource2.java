package com.zhisheng.function;

import com.zhisheng.common.model.WordEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Desc: 自定义数据源
 * Created by zhisheng on 2019-08-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomSource2 extends RichSourceFunction<WordEvent> {


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<WordEvent> ctx) throws Exception {
        for (int i = 0; i < 10; i++) {
            WordEvent element;
            if (i % 2 == 0) {
                element = new WordEvent(word(), count(), System.currentTimeMillis());
            } else {
                element = new WordEvent(word(), count(), System.currentTimeMillis() - 9000);
            }
            System.out.println(i + ":" + element);
            ctx.collect(element);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void cancel() {
    }


    private String word() {
        String[] strs = new String[]{"A", "B", "C", "D", "E", "F"};
        int index = (int) (Math.random() * strs.length);
        return "zhisheng" + strs[index];
    }

    private int count() {
        int[] strs = new int[]{1, 2, 3, 4, 5, 6};
        int index = (int) (Math.random() * strs.length);
        return strs[index];
    }

}
