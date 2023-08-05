package org.example.wh.SqlConnector;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

/**
 * @author wh
 * @create 2023/8/4 21:37
 */
public class MySink implements DynamicTableSink {
    private ReadableConfig options;
    private ResolvedSchema schema;

    public MySink(ReadableConfig options, ResolvedSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new MyPrintSinkFunction());
    }

    @Override
    public DynamicTableSink copy() {
        return new MySink(options,schema);
    }

    @Override
    public String asSummaryString() {
        return "Print Table Sink";
    }
}
