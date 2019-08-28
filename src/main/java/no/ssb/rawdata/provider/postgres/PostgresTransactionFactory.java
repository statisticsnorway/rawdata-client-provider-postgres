package no.ssb.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class PostgresTransactionFactory implements TransactionFactory {

    final HikariDataSource dataSource;

    public PostgresTransactionFactory(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends T> retryable, boolean readOnly) {
        return CompletableFuture.supplyAsync(() -> retryable.apply(createTransaction(readOnly)));
    }

    @Override
    public PostgresTransaction createTransaction(boolean readOnly) throws PersistenceException {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return new PostgresTransaction(connection);
        } catch (SQLException e) {
            throw new PersistenceException(e);
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
