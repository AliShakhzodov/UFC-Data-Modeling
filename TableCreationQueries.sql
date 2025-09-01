DROP TABLE IF EXISTS Fights CASCADE;
DROP TABLE IF EXISTS Fighters CASCADE;
DROP TABLE IF EXISTS Events CASCADE;
DROP TABLE IF EXISTS FighterStatsPerFight CASCADE;
DROP TABLE IF EXISTS Betting_Odds CASCADE;
DROP TABLE IF EXISTS Fighter_rankings CASCADE;
DROP TABLE IF EXISTS Fight_differentials CASCADE;

DROP TYPE IF EXISTS fighterStances;
DROP TYPE IF EXISTS weight_classes;
DROP TYPE IF EXISTS genders;
DROP TYPE IF EXISTS corners;

CREATE TYPE fighterStances AS ENUM ('Open Stance', 'Orthodox', 'Southpaw', 'Switch'); 
CREATE TYPE weight_classes AS ENUM ('Bantamweight', 'Catch Weight', 'Featherweight', 'Flyweight', 'Heavyweight', 
                                    'Light Heavyweight', 'Lightweight', 'Middleweight', 'Welterweight', 
									'Women''s Bantamweight', 'Women''s Featherweight', 'Women''s Flyweight', 
									'Women''s Strawweight');
CREATE TYPE genders AS ENUM ('FEMALE', 'MALE');
CREATE TYPE corners AS ENUM ('Blue', 'Red');

-- Fighter information that is unlikely to change throughout their career
CREATE TABLE Fighters(
  fighter_id SERIAL,
  fighter_name VARCHAR(100) NOT NULL UNIQUE,
  stance fighterStances,
  reach_cms DECIMAL(10,2),
  height_cms DECIMAL(10,2),
  gender genders,
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (fighter_id)
);

-- Events & Locations
CREATE TABLE Events(
  event_id SERIAL,
  event_date DATE NOT NULL,
  event_location VARCHAR(200),
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY(event_id)
);

-- Fight information
CREATE TABLE Fights(
  fight_id SERIAL,
  event_id INT,
  red_fighter_id INT NOT NULL,
  blue_fighter_id INT NOT NULL,
  title_bout BOOLEAN DEFAULT FALSE,
  num_rounds INT,
  winner_color VARCHAR(10),
  weight_class weight_classes, 
  finish_method VARCHAR(50),
  finish_details TEXT,
  finish_round INT,
  finish_round_time INT,
  total_fight_time_seconds INT,
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (fight_id),
  FOREIGN KEY (event_id) REFERENCES Events(event_id) ON DELETE CASCADE,
  FOREIGN KEY (redFighter_id) REFERENCES Fighters(fighter_id) ON DELETE CASCADE,
  FOREIGN KEY (blueFighter_id) REFERENCES Fighters(fighter_id) ON DELETE CASCADE
);

-- Fighter statistics at the time of fight
CREATE TABLE FighterStatsPerFight(
  stat_id SERIAL NOT NULL,
  fight_id INT NOT NULL,
  fighter_id INT,
  fighter_corner corners,

  -- Fighter statistics that vary
  weight_lbs INT,
  age INT,

  -- Current streaks & records
  current_win_streak INT,
  current_lose_streak INT,
  longest_win_streak INT,
  total_wins INT,
  total_losses INT,
  total_draws INT,

  -- Win methods
  wins_by_KO INT,
  wins_by_submission INT,
  wins_by_TKO_doctor_stoppage INT,
  wins_by_decision_unanimous INT,
  wins_by_decision_majority INT,
  wins_by_decision_split INT,

  -- Average performance
  avg_sig_strikes_landed DECIMAL(8,2),
  avg_sig_strikes_pct DECIMAL(5,2),
  avg_submission_attempts DECIMAL(5,2),
  avg_takedowns_landed DECIMAL(5,2),
  avg_takedowns_pct DECIMAL(5,2),

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (stat_id),
  FOREIGN KEY (fight_id) REFERENCES Fights(fight_id) ON DELETE CASCADE,
  FOREIGN KEY (fighter_id) REFERENCES Fighters(fighter_id) ON DELETE CASCADE
);

-- Betting info for each fight
CREATE TABLE Betting_Odds(
  odds_id SERIAL,
  fight_id INT NOT NULL,

  -- Main odds
  red_odds DECIMAL(8,2),
  blue_odds DECIMAL(8,2),
  red_expected_value DECIMAL(8,4),
  blue_expected_value DECIMAL(8,4),

  -- Method odds
  red_dec_odds DECIMAL(8,2),
  blue_dec_odds DECIMAL(8,2),
  red_KO_odds DECIMAL(8,2),
  blue_KO_odds DECIMAL(8,2),
  red_submission_odds DECIMAL(8,2),
  blue_submission_odds DECIMAL(8,2),
  PRIMARY KEY (odds_id),

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  FOREIGN KEY (fight_id) REFERENCES Fights(fight_id) ON DELETE CASCADE
);

-- Fighter rankings at time of fight
CREATE TABLE fighter_rankings (
    ranking_id SERIAL,
    fight_id INT,
    fighter_id INT,
    corner_color corners,
    
    -- General rankings
    pfp_rank INT,
    weight_class_rank INT,
    
    -- Weight class specific rankings
    flyweight_rank INT,
    bantamweight_rank INT,
    featherweight_rank INT,
    lightweight_rank INT,
    welterweight_rank INT,
    middleweight_rank INT,
    light_heavyweight_rank INT,
    heavyweight_rank INT,
    
    -- Women's rankings
    w_strawweight_rank INT,
    w_flyweight_rank INT,
    w_bantamweight_rank INT,
    w_featherweight_rank INT,
    
    better_rank BOOLEAN, -- True if the fighter has a better rank than their opponent
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	
	PRIMARY KEY (ranking_id),
	FOREIGN KEY (fight_id) REFERENCES Fights(fight_id) ON DELETE CASCADE,
	FOREIGN KEY (fighter_id) REFERENCES Fighters(fighter_id) ON DELETE CASCADE
);

-- Differentials between fighters pre-fight
CREATE TABLE Fight_differentials(
  differential_id SERIAL,
  fight_id INT NOT NULL,

  -- Streak differentials
  lose_streak_diff INT,
  win_streak_diff INT,
  longest_win_streak_diff INT,

  -- Record differentials
  wins_diff INT,
  losses_diff INT,
  draws_diff INT,
  total_rounds_diff INT,
  total_title_bouts_diff INT,
  KO_diff INT,
  submission_diff INT,

  -- Physical differentials
  height_cms_diff INT,
  reach_cms_diff INT,
  weight_lbs_diff INT,
  age_diff INT,

  -- Performance differentials
  sig_strikes_diff DECIMAL(8,2),
  avg_submission_att_diff DECIMAL(5,2),
  avg_takedown_att_diff DECIMAL(5,2),

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (differential_id),
  FOREIGN KEY (fight_id) REFERENCES Fights(fight_id) ON DELETE CASCADE
);