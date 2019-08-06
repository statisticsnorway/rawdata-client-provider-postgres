# rawdata-client-state-provider-postgres

## Start and initialize database

```
docker-compose up
```

```
./docker-postgres-init-db.bash
```

### Configuration

application.properties:

```
state.provider=postgres
postgres.driver.host=localhost
postgres.driver.port=5432
postgres.driver.user=rdc
postgres.driver.password=rdc
postgres.driver.database=rawdata_client
```
