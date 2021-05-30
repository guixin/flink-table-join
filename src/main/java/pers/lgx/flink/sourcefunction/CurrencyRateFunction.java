package pers.lgx.flink.sourcefunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

import static pers.lgx.flink.comment.CommentConstant.CURRENCY_ARRAY;

public class CurrencyRateFunction implements SourceFunction<Tuple2<String, Integer>> {

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        while (true) {
            Random random = new Random(System.currentTimeMillis());
            int index = random.nextInt(3);
            String currency = CURRENCY_ARRAY[index];
            int rate = random.nextInt(300);
            ctx.collect(new Tuple2<>(currency, rate));
            // 每10秒生成一次数据
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {

    }
}
