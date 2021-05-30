package pers.lgx.flink.lookupsource;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TemporalTableFunctionImpl;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;
import pers.lgx.flink.sourcefunction.CurrencyRateFunction;

import java.util.Collections;
import java.util.List;

public class TemporalTableSource implements LookupableTableSource<Row>, StreamTableSource<Row>, DefinedRowtimeAttributes {

    private final Table table;

    public TemporalTableSource(Table table) {
        this.table = table;
    }


    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return TemporalTableFunctionImpl.create(table.getQueryOperation(),
                ExpressionParser.parseExpression("proc_time"),
                ExpressionParser.parseExpression("r_currency"));
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        TableSchema schema = getTableSchema();

        final TypeInformation<?>[] schemaTypeInfos = schema.getFieldTypes();
        final String[] schemaFieldNames = schema.getFieldNames();

        return new RowTypeInfo(schemaTypeInfos, schemaFieldNames);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return null;
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public TableSchema getTableSchema() {
        return table.getSchema();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return execEnv.addSource(new CurrencyRateFunction()).map(t2 -> Row.of(t2.f0, t2.f1));
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
                "proc_time",
                new ExistingField("proc_time"),
                new AscendingTimestamps());
        List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
        return listRowtimeAttrDescr;
    }
}
