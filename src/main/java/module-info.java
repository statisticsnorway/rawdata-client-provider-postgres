import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.provider.postgres.PostgresRawdataClientInitializer;

module no.ssb.rawdata.provider.postgres {
    requires java.logging;
    requires org.slf4j;
    requires java.sql;
    requires com.zaxxer.hikari;
    requires postgresql;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;

    provides RawdataClientInitializer with PostgresRawdataClientInitializer;

    opens postgres;

}
