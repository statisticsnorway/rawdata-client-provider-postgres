DROP TABLE IF EXISTS content;
DROP TABLE IF EXISTS positions;
DROP TABLE IF EXISTS subscription;

CREATE TABLE positions
(
    id        serial                   NOT NULL,
    topic     varchar                  NOT NULL,
    opaque_id varchar                  NOT NULL,
    ts        timestamp with time zone NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (topic, opaque_id)
);

CREATE TABLE content
(
    id             serial  NOT NULL,
    position_fk_id int     NOT NULL, /* TODO CREATE INDEX ON THIS FIELD */
    name           varchar NOT NULL,
    data           bytea   NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (position_fk_id) REFERENCES positions (id)
);

CREATE TABLE subscription
(
    topic        varchar NOT NULL,
    subscription varchar NOT NULL,
    position     int     NOT NULL,
    PRIMARY KEY (topic, subscription)
);
