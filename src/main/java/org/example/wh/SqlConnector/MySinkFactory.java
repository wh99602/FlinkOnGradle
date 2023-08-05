package org.example.wh.SqlConnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.types.Row;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wh
 * @create 2023/8/4 21:11
 */
public class MySinkFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        // options and schema
        return new MySink(options, schema);
    }

    @Override
    public String factoryIdentifier() {
        return WithOption.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.add(WithOption.HOME);
        return configOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.add(WithOption.BATCH_SIZE);
        return configOptions;
    }
}
