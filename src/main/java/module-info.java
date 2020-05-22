import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.provider.postgres.PostgresRawdataClientInitializer;

module no.ssb.rawdata.postgres {
    requires java.logging;
    requires org.slf4j;
    requires java.sql;
    requires de.huxhorn.sulky.ulid;
    requires com.zaxxer.hikari;
    requires org.postgresql.jdbc;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;

    opens no.ssb.rawdata.provider.postgres.init;

    provides RawdataClientInitializer with PostgresRawdataClientInitializer;
}
