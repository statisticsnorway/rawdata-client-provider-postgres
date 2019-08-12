DROP TABLE IF EXISTS content;
DROP TABLE IF EXISTS positions;

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

CREATE INDEX ON content (position_fk_id, name);
