package no.ssb.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;
import no.ssb.service.provider.api.ProviderName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

@ProviderName("postgres")
public class PostgresRawdataClientInitializer implements RawdataClientInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresRawdataClientInitializer.class);

    @Override
    public String providerId() {
        return "postgres";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "postgres.driver.host",
                "postgres.driver.port",
                "postgres.driver.user",
                "postgres.driver.password",
                "postgres.driver.database"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configMap) {
        boolean disablePostgresDatabase = Boolean.parseBoolean(configMap.get("postgres.driver.disabled"));

        if (!disablePostgresDatabase) {
            HikariDataSource dataSource = openPostgresDataSource(
                    configMap.get("postgres.driver.host"),
                    configMap.get("postgres.driver.port"),
                    configMap.get("postgres.driver.user"),
                    configMap.get("postgres.driver.password"),
                    configMap.get("postgres.driver.database"),
                    Boolean.parseBoolean(configMap.get("postgres.dropOrCreateDb")));

            return new PostgresRawdataClient(new PostgresTransactionFactory(dataSource));

        } else {
            HikariDataSource dataSource = openH2DataSource(
                    configMap.get("h2.driver.url"),
                    "sa",
                    "sa",
                    Boolean.parseBoolean(configMap.get("postgres.dropOrCreateDb"))
            );

            try {
                Class<? extends TransactionFactory> transactionFactoryClass = (Class<? extends TransactionFactory>) Class.forName("no.ssb.rawdata.provider.postgres.H2TransactionFactory");
                TransactionFactory transactionFactory = transactionFactoryClass.getDeclaredConstructor(HikariDataSource.class).newInstance(dataSource);
                return new PostgresRawdataClient(transactionFactory);

            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // https://github.com/brettwooldridge/HikariCP
    static HikariDataSource openPostgresDataSource(String postgresDbDriverHost, String postgresDbDriverPort, String postgresDbDriverUser, String postgresDbDriverPassword, String postgresDbDriverDatabase, boolean dropOrCreateDb) {
        LOG.info("Configured Database: postgres");
        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", postgresDbDriverHost);
        props.setProperty("dataSource.portNumber", postgresDbDriverPort);
        props.setProperty("dataSource.user", postgresDbDriverUser);
        props.setProperty("dataSource.password", postgresDbDriverPassword);
        props.setProperty("dataSource.databaseName", postgresDbDriverDatabase);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        if (dropOrCreateDb) {
            dropOrCreateDatabase(datasource);
        }

        return datasource;
    }

    static HikariDataSource openH2DataSource(String jdbcUrl, String username, String password, boolean dropOrCreateDb) {
        LOG.info("Configured Database: h2");
        Properties props = new Properties();
        props.setProperty("jdbcUrl", jdbcUrl);
        props.setProperty("username", username);
        props.setProperty("password", password);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        if (dropOrCreateDb) {
            dropOrCreateDatabase(datasource);
        }

        return datasource;
    }


    static void dropOrCreateDatabase(HikariDataSource datasource) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource("postgres/init-db.sql");
//            System.out.printf("initSQL: %s%n", initSQL);
            Connection conn = datasource.getConnection();
            conn.beginRequest();

            try (Scanner s = new Scanner(initSQL)) {
                s.useDelimiter("(;(\r)?\n)|(--\n)");
                try (Statement st = conn.createStatement()) {
                    try {
                        while (s.hasNext()) {
                            String line = s.next();
                            if (line.startsWith("/*!") && line.endsWith("*/")) {
                                int i = line.indexOf(' ');
                                line = line.substring(i + 1, line.length() - " */".length());
                            }

                            if (line.trim().length() > 0) {
                                st.execute(line);
                            }
                        }
                        conn.commit();
                    } finally {
                        st.close();
                    }
                }
            }

            conn.endRequest();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
