package no.ssb.rawdata.provider.postgres.tx;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

public interface Transaction extends AutoCloseable {

    Connection connection();

    CompletableFuture<Void> commit();

    CompletableFuture<Void> cancel();

    @Override
    default void close() {
        boolean committed = false;
        try {
            commit().join();
            committed = true;
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new RuntimeException(t);
        } finally {
            if (!committed) {
                cancel().join();
            }
        }
    }
}
