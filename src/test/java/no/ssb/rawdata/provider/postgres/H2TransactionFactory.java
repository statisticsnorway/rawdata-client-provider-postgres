package no.ssb.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class H2TransactionFactory implements TransactionFactory {

    final HikariDataSource dataSource;

    public H2TransactionFactory(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends T> retryable, boolean readOnly) {
        return CompletableFuture.supplyAsync(() -> retryable.apply(createTransaction(readOnly)));
    }

    @Override
    public H2Transaction createTransaction(boolean readOnly) throws PersistenceException {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return new H2Transaction(connection);
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean checkIfTableTopicExists(String topic, String table) {
        try {
            Connection conn = dataSource.getConnection();
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
