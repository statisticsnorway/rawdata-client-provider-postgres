package no.ssb.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class H2TransactionFactory implements TransactionFactory {

    final HikariDataSource dataSource;

    public H2TransactionFactory(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public H2Transaction createTransaction(boolean readOnly) throws PersistenceException {
        Connection connection = null;
        H2Transaction h2Transaction = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            h2Transaction = new H2Transaction(connection);
            return h2Transaction;
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } finally {
            if (h2Transaction == null && connection != null) {
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
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.getMetaData().getTables(null, null, topic + "_" + table, null);
            return rs.next();

        } catch (SQLException e) {
            throw new PersistenceException(e);
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
