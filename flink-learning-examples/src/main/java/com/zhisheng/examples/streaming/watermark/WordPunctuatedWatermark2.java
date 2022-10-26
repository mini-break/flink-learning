package com.zhisheng.examples.streaming.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Desc: Punctuated Watermark
 */
public class WordPunctuatedWatermark2 implements WatermarkStrategy<Word> {

    @Override
    public WatermarkGenerator<Word> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Word>(){

            /**
             * 如果生成的watermark是null，或者小于之前的watermark，则该watermark不会发往下游
             * @param event
             * @param eventTimestamp
             * @param output
             */
            @Nullable
            @Override
            public void onEvent(Word event, long eventTimestamp, WatermarkOutput output) {
                if(eventTimestamp % 3 == 0){
                    new Watermark(eventTimestamp);
                }
                // output.emitWatermark(eventTimestamp % 3 == 0 ? new Watermark(eventTimestamp) : null);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }

        };
    }

    // @Override
    // public TimestampAssigner<Word> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
    //     return WatermarkStrategy.super.createTimestampAssigner(context);
    // }
    //
    // @Override
    // public WatermarkStrategy<Word> withTimestampAssigner(TimestampAssignerSupplier<Word> timestampAssigner) {
    //     return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    // }
    //
    // @Override
    // public WatermarkStrategy<Word> withTimestampAssigner(SerializableTimestampAssigner<Word> timestampAssigner) {
    //     return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    // }
    //
    // @Override
    // public WatermarkStrategy<Word> withIdleness(Duration idleTimeout) {
    //     return WatermarkStrategy.super.withIdleness(idleTimeout);
    // }
}
