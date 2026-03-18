DROP TABLE IF EXISTS q4_results;

CREATE TABLE q4_results (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
