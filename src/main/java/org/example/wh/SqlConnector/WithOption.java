package org.example.wh.SqlConnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author wh
 * @create 2023/8/5 10:02
 */
public class WithOption {
    public static final String IDENTIFIER = "wh";
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batch-size").intType().noDefaultValue();
    public static final ConfigOption<String> HOME = ConfigOptions.key("home").stringType().noDefaultValue();
}
