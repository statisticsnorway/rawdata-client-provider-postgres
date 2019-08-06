package no.ssb.rawdata.provider.postgres;

import java.io.Closeable;

public interface Disposable extends Closeable {

    void cancel();

    void dispose();

    @Override
    void close();
}
