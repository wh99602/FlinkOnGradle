package org.example.wh.modifysourcecode;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.JdbcOutputFormatBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.example.wh.SqlConnector.MyPrintSinkFunction;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;


/**
 * @author wh
 * @create 2023/8/5 14:47
 */
public class MySink implements DynamicTableSink {

    private final MyConnectorOptions jdbcOptions;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcDmlOptions dmlOptions;
    private final DataType physicalRowDataType;
    private final String dialectName;

    public MySink(
            MyConnectorOptions jdbcOptions,
            JdbcExecutionOptions executionOptions,
            JdbcDmlOptions dmlOptions,
            DataType physicalRowDataType) {
        this.jdbcOptions = jdbcOptions;
        this.executionOptions = executionOptions;
        this.dmlOptions = dmlOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = dmlOptions.getDialect().dialectName();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || dmlOptions.getKeyFields().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInformation =
                context.createTypeInformation(physicalRowDataType);
        final MyOutputFormatBuilder builder = new MyOutputFormatBuilder();

        builder.setJdbcOptions(jdbcOptions);
        builder.setJdbcDmlOptions(dmlOptions);
        builder.setJdbcExecutionOptions(executionOptions);
        builder.setRowDataTypeInfo(rowDataTypeInformation);
        builder.setFieldDataTypes(
                DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]));
        if(jdbcOptions.getWritenull()){
        return SinkFunctionProvider.of(
                new MySinkFunction<>(builder.build()), jdbcOptions.getParallelism());
        }else {
            return SinkFunctionProvider.of(new MyPrintSinkFunction(), jdbcOptions.getParallelism());
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new MySink(
                jdbcOptions, executionOptions, dmlOptions, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySink)) {
            return false;
        }
        MySink that = (MySink) o;
        return Objects.equals(jdbcOptions, that.jdbcOptions)
                && Objects.equals(executionOptions, that.executionOptions)
                && Objects.equals(dmlOptions, that.dmlOptions)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jdbcOptions, executionOptions, dmlOptions, physicalRowDataType, dialectName);
    }
}
