package no.ssb.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresTransactionFactory implements TransactionFactory {

    final HikariDataSource dataSource;

    public PostgresTransactionFactory(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public PostgresTransaction createTransaction(boolean readOnly) throws PersistenceException {
        PostgresTransaction postgresTransaction = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            postgresTransaction = new PostgresTransaction(connection);
            return postgresTransaction;
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } finally {
            if (postgresTransaction == null && connection != null) {
                // exceptional return, do not leave connection open
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }
        }
    }

    @Override
    public boolean checkIfTableTopicExists(String topic, String table) {
        try (Transaction tx = createTransaction(true)) {
            PreparedStatement ps = tx.connection().prepareStatement("SELECT 1 FROM pg_tables WHERE schemaname = ? AND tablename = ?");
            ps.setString(1, "public");
            ps.setString(2, topic + "_" + table);
            ResultSet rs = ps.executeQuery();
            return rs.next();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public DataSource dataSource() {
        return dataSource;
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
