import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Desc: 窗口生成测试（开始时间戳、结束时间戳）
 * Created by zhisheng on 2019/11/4 上午12:11
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class TestWindowSize {
    public static void main(String[] args) {
        long l = System.currentTimeMillis();
        //timestamp - (timestamp - offset + slide) % slide;
        System.out.println(l - (l  + 60 * 1000) %  60000);

        // 窗口大小为24小时
        long size = Time.hours(24).toMilliseconds();
        long slide = Time.hours(1).toMilliseconds();
        long lastStart = (1572794063000l - (1572794063000l + slide) % slide);
        for (long start = lastStart; start > 1572794063000l - size; start -= slide) {
            System.out.println(start + "  " + (start + size));
        }
    }
}
