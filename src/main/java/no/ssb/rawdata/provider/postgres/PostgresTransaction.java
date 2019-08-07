package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionStatistics;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

class PostgresTransaction implements Transaction {

    final Connection connection;
    final TransactionStatistics statistics = new TransactionStatistics();

    public PostgresTransaction(Connection connection) throws SQLException {
        this.connection = connection;
        connection.beginRequest();
    }

    @Override
    public CompletableFuture<TransactionStatistics> commit() throws PersistenceException {
        try {
            return CompletableFuture.completedFuture(statistics);
        } finally {
            try {
                connection.commit();
                connection.endRequest();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        try {
            return CompletableFuture.completedFuture(statistics);
        } finally {
            try {
                connection.rollback();
                connection.endRequest();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
    }
}
