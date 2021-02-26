DROP TABLE IF EXISTS "TOPIC_metadata";

CREATE TABLE "TOPIC_metadata"
(
    name varchar NOT NULL,
    data bytea   NOT NULL,
    PRIMARY KEY (name)
);
