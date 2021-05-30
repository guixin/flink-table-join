/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pers.lgx.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import pers.lgx.flink.comment.RegisterTable;

public class TemporalFunctionJoinJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		RegisterTable.registerTable(env, tEnv);

		Table ratesHistory = tEnv.scan("RatesHistory");
		TemporalTableFunction temporalTableFunction = ratesHistory.createTemporalTableFunction("proc_time", "r_currency");
		tEnv.registerFunction("rates", temporalTableFunction);
		Table orders = tEnv.scan("Orders");

//		Table result = orders
//				.joinLateral("rates(o_time)", "o_currency = r_currency")
//				.select("o_order_no, o_currency, o_amount, o_amount * COALESCE(r_rate, 1) / 100 as r_rmb_amount, " +
//						"o_time");
		Table result = orders
				.joinLateral("rates(o_time)", "o_currency = r_currency")
				.select("o_order_no, o_currency, o_amount, o_amount * r_rate / 100 as r_rmb_amount, " +
						"o_time");

		result.printSchema();

		JDBCAppendTableSink jdbcSink = jdbcSink(args);

		tEnv.registerTableSink("jdbcSink", new String[] {"o_order_no", "o_currency", "o_amount", "r_rmb_amount", "o_time"},
				new TypeInformation[] {Types.STRING, Types.STRING, Types.INT, Types.INT, Types.SQL_TIMESTAMP}, jdbcSink);
		result.insertInto("jdbcSink");

		env.execute("Flink Table Join Job");
	}

	public static JDBCAppendTableSink jdbcSink(String[] args) {
		return JDBCAppendTableSink.builder()
				.setDBUrl(String.format("jdbc:mysql://%s:3306/%s", args[0], args[1]))
				.setDrivername("com.mysql.cj.jdbc.Driver")
				.setPassword(args[2])
				.setUsername(args[3])
				.setQuery("insert into t_order_result (order_no, currency, amount, rmb_amount, time) value (?, ?, ?, ?, ?)")
				.setParameterTypes(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP)
				.setBatchSize(5)
				.build();
	}
}
