package org.example.wh.SqlConnector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

/**
 * @author wh
 * @create 2023/8/5 10:22
 */
public class MyPrintSinkFunction extends RichSinkFunction<RowData>{
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        System.out.println(value);
    }
}
