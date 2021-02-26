package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class PostgresRawdataMetadataClient implements RawdataMetadataClient {

    final String topic;
    final TransactionFactory transactionFactory;

    public PostgresRawdataMetadataClient(String topic, TransactionFactory transactionFactory) {
        this.topic = topic;
        this.transactionFactory = transactionFactory;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        try (Transaction tx = transactionFactory.createTransaction(false)) {
            try (PreparedStatement st = tx.connection().prepareStatement(String.format("SELECT name FROM \"%s_metadata\"", topic))) {
                Set<String> keys = new LinkedHashSet<>();
                try (ResultSet rs = st.executeQuery()) {
                    if (!rs.next()) {
                        return Collections.emptySet();
                    }
                    do {
                        String key = rs.getString(1);
                        keys.add(key);
                    } while (rs.next());
                }
                return keys;
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public byte[] get(String key) {
        try (Transaction tx = transactionFactory.createTransaction(false)) {
            try (PreparedStatement st = tx.connection().prepareStatement(String.format("SELECT data FROM \"%s_metadata\" WHERE name = ?", topic))) {
                st.setString(1, key);
                try (ResultSet rs = st.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    byte[] data = rs.getBytes(1);
                    return data;
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public PostgresRawdataMetadataClient put(String key, byte[] value) {
        try (Transaction tx = transactionFactory.createTransaction(false)) {
            boolean alreadyExists = false;
            try (PreparedStatement st = tx.connection().prepareStatement(String.format("SELECT 1 FROM \"%s_metadata\" WHERE name = ?", topic))) {
                st.setString(1, key);
                try (ResultSet rs = st.executeQuery()) {
                    if (rs.next()) {
                        alreadyExists = true;
                    }
                }
            }
            if (alreadyExists) {
                try (PreparedStatement st = tx.connection().prepareStatement(String.format("UPDATE \"%s_metadata\" SET data = ? WHERE name = ?", topic))) {
                    st.setBytes(1, value);
                    st.setString(2, key);
                    st.executeUpdate();
                }
            } else {
                try (PreparedStatement st = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_metadata\" (name, data) VALUES (?, ?)", topic))) {
                    st.setString(1, key);
                    st.setBytes(2, value);
                    st.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
        return this;
    }

    @Override
    public RawdataMetadataClient remove(String key) {
        try (Transaction tx = transactionFactory.createTransaction(false)) {
            try (PreparedStatement st = tx.connection().prepareStatement(String.format("DELETE FROM \"%s_metadata\" WHERE name = ?", topic))) {
                st.setString(1, key);
                st.executeUpdate();
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
        return this;
    }
}
