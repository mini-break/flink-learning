package com.zhisheng.examples.streaming.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * @author hg
 * @date 2022-10-24 14:23:13
 * @description: TODO
 */
public class WordSerializableTimestampAssigner implements SerializableTimestampAssigner<Word> {
    @Override
    public long extractTimestamp(Word element, long recordTimestamp) {
        return element.getTimestamp();
    }
}
