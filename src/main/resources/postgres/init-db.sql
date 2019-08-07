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
    position_fk_id int     NOT NULL,
    name           varchar NOT NULL,
    data           bytea   NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (position_fk_id) REFERENCES positions (id)
);

CREATE TABLE subscription
(
    id           serial  NOT NULL,
    topic        varchar NOT NULL,
    subscription varchar NOT NULL,
    position     varchar NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (topic, subscription, position)
);
