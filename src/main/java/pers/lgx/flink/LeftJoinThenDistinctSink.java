package pers.lgx.flink;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import pers.lgx.flink.comment.RegisterTable;

public class LeftJoinThenDistinctSink {
    public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        RegisterTable.registerTable(env, tEnv);

        // order_no, currency, amount, rmb_amount, time
        Table result = tEnv.sqlQuery("select o_order_no as order_no, o_currency as currency, o_amount as amount, " +
                "o_amount * r_rate / 100 as rmb_amount, o_time from Orders" +
                " left join RatesHistory on o_currency = r_currency");


        JDBCUpsertTableSink jdbcSink = jdbcSink(args);
        jdbcSink.setKeyFields(new String[] {"order_no"});

        tEnv.registerTableSink("jdbcSink", new String[]{"order_no", "currency", "amount", "rmb_amount", "o_time"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.INT, Types.SQL_TIMESTAMP}, jdbcSink);
        result.insertInto("jdbcSink");
    }

    public static JDBCUpsertTableSink jdbcSink(String... args) {
        return new JDBCUpsertTableSink.Builder()
                .setMaxRetryTimes(1)
                .setTableSchema(TableSchema.builder().field("order_no", DataTypes.STRING()).field("currency",
                        DataTypes.STRING())
                        .field("amount", DataTypes.INT()).field("rmb_amount", DataTypes.INT())
                        .field("o_time", SqlTimeTypeInfo.TIMESTAMP)
                        .build())
                .setOptions(JDBCOptions.builder().setTableName("t_order_result_v2")
                        .setDBUrl(String.format("jdbc:mysql://%s:3306/%s", args[0], args[1]))
                        .setDriverName("com.mysql.cj.jdbc.Driver").setPassword(args[2]).setUsername(args[3])
                        .build())
                .setFlushIntervalMills(5000)
                .setFlushMaxSize(1)
                .build();
    }
}
