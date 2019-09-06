DROP TABLE IF EXISTS "TOPIC_content";
DROP TABLE IF EXISTS "TOPIC_positions";

CREATE TABLE "TOPIC_positions"
(
    ulid            uuid                     NOT NULL,
    ordering_group  varchar                      NULL,
    sequence_number bigint                   NOT NULL,
    position        varchar                  NOT NULL,
    ts              timestamp with time zone NOT NULL,
    PRIMARY KEY (ulid)
);

CREATE TABLE "TOPIC_content"
(
    ulid uuid    NOT NULL,
    name varchar NOT NULL,
    data bytea   NOT NULL,
    PRIMARY KEY (ulid, name)
);
