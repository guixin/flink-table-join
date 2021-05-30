package pers.lgx.flink.comment;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import pers.lgx.flink.sourcefunction.CurrencyRateFunction;
import pers.lgx.flink.sourcefunction.OrderFunction;

public class RegisterTable {
    public static void registerTable(final StreamExecutionEnvironment env, final StreamTableEnvironment tEnv) {
        DataStream<Tuple2<String, Integer>> currencyRateDs = env.addSource(new CurrencyRateFunction());
        DataStream<Tuple3<String, String, Integer>> orderDs = env.addSource(new OrderFunction());

        Table ratesHistory = tEnv.fromDataStream(currencyRateDs, "r_currency, r_rate, proc_time.proctime");
        tEnv.registerTable("RatesHistory", ratesHistory);

        Table orders = tEnv.fromDataStream(orderDs, "o_order_no, o_currency, o_amount, o_time.proctime");
        tEnv.registerTable("Orders", orders);
    }
}
