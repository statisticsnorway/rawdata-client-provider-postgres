DROP TABLE IF EXISTS content;
DROP TABLE IF EXISTS position;
DROP TABLE IF EXISTS subscription;

CREATE TABLE position
(
    id       serial                   NOT NULL,
    topic    varchar                  NOT NULL,
    position varchar                  NOT NULL,
    ts       timestamp with time zone NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (topic, position)
);

CREATE TABLE content
(
    id             int     NOT NULL,
    position_fk_id varchar NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (position_fk_id) REFERENCES position (id)
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
