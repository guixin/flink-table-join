package pers.lgx.flink.sourcefunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

import static pers.lgx.flink.comment.CommentConstant.CURRENCY_ARRAY;

public class OrderFunction implements SourceFunction<Tuple3<String, String, Integer>> {
    @Override
    public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {
        while (true) {
            Random random = new Random(System.currentTimeMillis());
            int index = random.nextInt(4);
            String currency = CURRENCY_ARRAY[index];
            int amount = random.nextInt(300);
            String orderNo = UUID.randomUUID().toString();
            ctx.collect(new Tuple3<>(orderNo, currency, amount));
            // 每 2 秒生成一条数据
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {

    }
}
