# Flink Table join

假设有个汇率表，是兑 RMB 的汇率

| currency | rate | timestamp |
| --- | --- | --- |
| Euro | 102 | 1621941908|

有个订单表

| order_id | amount | timestamp |
| --- | --- | --- |
| 1408755e-bd4c-11eb-8529-0242ac130003 | 10 | 1621941908 |

然后两个表进行 join，生成订单当前时间的汇率所对应的 RMB 金额是多少。最后 sink 到数据库中


```java
        JDBCAppendTableSink jdbcSink = JDBCAppendTableSink.builder()
                .setDBUrl(String.format("jdbc:mariadb://%s:3306/%s", args[0], args[1]))
                .setDrivername("org.mariadb.jdbc.Driver")
                .setPassword(args[2])
                .setUsername(args[3])
                .setQuery("insert into t_doctor_relate_patient_name (doctor_uin, patient_uin, name)" +
                        " value (?, ?, ?)")
                .setParameterTypes(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO)
                .setBatchSize(1)
                .build();
```
