DROP TABLE IF EXISTS "TOPIC_content";
DROP TABLE IF EXISTS "TOPIC_positions";

CREATE TABLE "TOPIC_positions"
(
    id        serial                   NOT NULL,
    opaque_id varchar                  NOT NULL,
    ts        timestamp with time zone NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (opaque_id)
);

CREATE TABLE "TOPIC_content"
(
    position_fk_id int     NOT NULL,
    name           varchar NOT NULL,
    data           bytea   NOT NULL,
    PRIMARY KEY (position_fk_id, name),
    FOREIGN KEY (position_fk_id) REFERENCES "TOPIC_positions" (id)
);
