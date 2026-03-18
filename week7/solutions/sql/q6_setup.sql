DROP TABLE IF EXISTS q6_results;

CREATE TABLE q6_results (
    window_start TIMESTAMP PRIMARY KEY,
    total_tip_amount DOUBLE PRECISION
);
