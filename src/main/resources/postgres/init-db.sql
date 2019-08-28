DROP TABLE IF EXISTS "TOPIC_content";
DROP TABLE IF EXISTS "TOPIC_positions";

CREATE TABLE "TOPIC_positions"
(
    ulid      uuid                     NOT NULL,
    opaque_id varchar                  NOT NULL,
    ts        timestamp with time zone NOT NULL,
    PRIMARY KEY (ulid),
    UNIQUE (opaque_id)
);

CREATE TABLE "TOPIC_content"
(
    position_fk_ulid uuid    NOT NULL,
    name             varchar NOT NULL,
    data             bytea   NOT NULL,
    PRIMARY KEY (position_fk_ulid, name)
);
