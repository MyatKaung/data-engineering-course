DROP TABLE IF EXISTS q5_results;

CREATE TABLE q5_results (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
