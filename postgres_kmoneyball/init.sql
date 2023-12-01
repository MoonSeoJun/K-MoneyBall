CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100),
    date_of_birth DATE,
    nationality VARCHAR(50)
    -- Other player-related columns
);

CREATE TABLE clubs (
    club_id SERIAL PRIMARY KEY,
    club_name VARCHAR(100),
    league VARCHAR(100),
    club_url VARCHAR(100)
    -- Other club-related columns
);


CREATE TABLE matches (
    match_id SERIAL PRIMARY KEY,
    match_date DATE,
    player_id INT REFERENCES players(player_id),
    club_id INT REFERENCES clubs(club_id),
    goals INT,
    assists INT,
    minutes_played INT
    -- Other match-related columns
);