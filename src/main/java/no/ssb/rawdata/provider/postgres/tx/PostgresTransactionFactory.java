package no.ssb.rawdata.provider.postgres.tx;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.rawdata.provider.postgres.PersistenceException;

import java.sql.Connection;
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
    public void close() {
        dataSource.close();
    }
}
