package org.example.wh.modifysourcecode;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author wh
 * @create 2023/8/6 12:50
 */
public class MyConnectorOptions extends JdbcConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final JdbcDialect dialect;
    private final @Nullable Integer parallelism;

    private final Boolean writenull;

    private MyConnectorOptions(
            String dbURL,
            String tableName,
            @Nullable String driverName,
            @Nullable String username,
            @Nullable String password,
            JdbcDialect dialect,
            @Nullable Integer parallelism,
            int connectionCheckTimeoutSeconds,
            Boolean writenull) {
        super(dbURL, driverName, username, password, connectionCheckTimeoutSeconds);
        this.tableName = tableName;
        this.dialect = dialect;
        this.parallelism = parallelism;
        this.writenull=writenull;
    }

    public Boolean getWritenull() {
        return writenull;
    }

    public String getTableName() {
        return tableName;
    }

    public JdbcDialect getDialect() {
        return dialect;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public static MyConnectorOptions.Builder builder() {
        return new MyConnectorOptions.Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MyConnectorOptions) {
            MyConnectorOptions options = (MyConnectorOptions) o;
            return Objects.equals(url, options.url)
                    && Objects.equals(tableName, options.tableName)
                    && Objects.equals(driverName, options.driverName)
                    && Objects.equals(username, options.username)
                    && Objects.equals(password, options.password)
                    && Objects.equals(
                    dialect.getClass().getName(), options.dialect.getClass().getName())
                    && Objects.equals(parallelism, options.parallelism)
                    && Objects.equals(
                    connectionCheckTimeoutSeconds, options.connectionCheckTimeoutSeconds);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                url,
                tableName,
                driverName,
                username,
                password,
                dialect.getClass().getName(),
                parallelism,
                connectionCheckTimeoutSeconds);
    }

    /** Builder of {@link MyConnectorOptions}. */
    public static class Builder {
        private ClassLoader classLoader;
        private String dbURL;
        private String tableName;
        private String driverName;
        private String username;
        private String password;
        private JdbcDialect dialect;
        private Integer parallelism;
        private int connectionCheckTimeoutSeconds = 60;
        private Boolean writenull;

        /**
         * optional, specifies the classloader to use in the planner for load the class in user jar.
         *
         * <p>By default, this is configured using {@code
         * Thread.currentThread().getContextClassLoader()}.
         *
         * <p>Modify the {@link ClassLoader} only if you know what you're doing.
         */
        public MyConnectorOptions.Builder setWritenull(Boolean writenull) {
            this.writenull = writenull;
            return this;
        }
        public MyConnectorOptions.Builder setClassLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        /** required, table name. */
        public MyConnectorOptions.Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /** optional, user name. */
        public MyConnectorOptions.Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /** optional, password. */
        public MyConnectorOptions.Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /** optional, connectionCheckTimeoutSeconds. */
        public MyConnectorOptions.Builder setConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        /**
         * optional, driver name, dialect has a default driver name, See {@link
         * JdbcDialect#defaultDriverName}.
         */
        public MyConnectorOptions.Builder setDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        /** required, JDBC DB url. */
        public MyConnectorOptions.Builder setDBUrl(String dbURL) {
            this.dbURL = dbURL;
            return this;
        }

        /**
         * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by {@link
         * JdbcDialectLoader#load} from DB url.
         */
        public MyConnectorOptions.Builder setDialect(JdbcDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public MyConnectorOptions.Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public MyConnectorOptions build() {
            checkNotNull(dbURL, "No dbURL supplied.");
            checkNotNull(tableName, "No tableName supplied.");
            if (this.dialect == null) {
                if (classLoader == null) {
                    classLoader = Thread.currentThread().getContextClassLoader();
                }
                this.dialect = JdbcDialectLoader.load(dbURL, classLoader);
            }
            if (this.driverName == null) {
                Optional<String> optional = dialect.defaultDriverName();
                this.driverName =
                        optional.orElseThrow(
                                () -> new NullPointerException("No driverName supplied."));
            }

            return new MyConnectorOptions(
                    dialect.appendDefaultUrlProperties(dbURL),
                    tableName,
                    driverName,
                    username,
                    password,
                    dialect,
                    parallelism,
                    connectionCheckTimeoutSeconds,
                    writenull);
        }
    }
}
