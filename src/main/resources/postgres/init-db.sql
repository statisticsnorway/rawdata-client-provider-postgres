DROP TABLE IF EXISTS completed_positions;

CREATE TABLE completed_positions (
  id            serial                       NOT NULL,
  namespace     varchar                      NOT NULL,
  opaque_id     varchar                      NOT NULL,
  ts            timestamp with time zone     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (namespace, opaque_id)
);
