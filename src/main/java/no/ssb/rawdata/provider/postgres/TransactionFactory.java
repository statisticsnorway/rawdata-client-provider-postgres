package no.ssb.rawdata.provider.postgres;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface TransactionFactory {

    /**
     * Execute a unit-of-work within a new transaction asynchronously.
     *
     * @param retryable a function that will be run in another thread asynchronously given a new transaction as input.
     * @param <T>       The result type of the retryable function.
     * @param readOnly  whether the unit-of-work represent a read-only transaction or not.
     * @return a completable future that will be signalled when the asynchronous work is complete.
     */
    default <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends T> retryable, boolean readOnly) {
        return CompletableFuture.supplyAsync(() -> {
            try (Transaction tx = createTransaction(readOnly)) {
                return retryable.apply(tx);
            }
        });
    }

    /**
     * Create a new transaction.
     *
     * @param readOnly true if the transaction will only perform read operations, false if at least one write operation
     *                 will be performed, and false if the caller is unsure. Note that the underlying persistence
     *                 provider may be able to optimize performance and contention related issues when read-only
     *                 transactions are involved.
     * @return the newly created transaction
     * @throws PersistenceException
     */
    Transaction createTransaction(boolean readOnly) throws PersistenceException;

    /**
     * Close all resources associated with this transaction-factory. Such resources will typically be open-transactions,
     * thread-pools, data-source connections, or other persistence-provider specific resources.
     */
    void close();
}
