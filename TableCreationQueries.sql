DROP TABLE IF EXISTS Fights;
DROP TABLE IF EXISTS Fighters;
DROP TABLE IF EXISTS Stats;
DROP TABLE IF EXISTS Events;

DROP TYPE IF EXISTS fighterStances;
DROP TYPE IF EXISTS weight_classes;

CREATE TYPE fighterStances AS ENUM ('Open Stance', 'Orthodox', 'Southpaw', 'Switch');
CREATE TYPE weight_classes AS ENUM ('Bantamweight', 'Catch Weight', 'Featherweight', 'Flyweight', 'Heavyweight', 
                                    'Light Heavyweight', 'Lightweight', 'Middleweight', 'Welterweight', 
									'Women''s Bantamweight', 'Women''s Featherweight', 'Women''s Flyweight', 
									'Women''s Strawweight');

CREATE TABLE Fighters(
  fighter_id INT,
  name VARCHAR(100),
  stance fighterStances,
  reach DECIMAL(10,2),
  height DECIMAL(10,2),
  age INT,
  win_rate INT,
  PRIMARY KEY (fighter_id)
);

CREATE TABLE Fights(
  fight_id INT NOT NULL,
  fight_date DATE,
  fighter_1_id INT,
  fighter_2_id INT,
  weight_class weight_classes, 
  fight_method VARCHAR(50),
  winner_id INT,
  PRIMARY KEY (fight_id),
  FOREIGN KEY (fighter_1_id) REFERENCES Fighters(fighter_id) ON DELETE CASCADE,
  FOREIGN KEY (fighter_2_id) REFERENCES Fighters(fighter_id) ON DELETE CASCADE
);

CREATE TABLE Stats(
  fight_id INT NOT NULL,
  strikes INT,
  takedowns INT,
  submission_attempts INT,
  PRIMARY KEY (fight_id),
  FOREIGN KEY (fight_id) REFERENCES Fights(fight_id) ON DELETE CASCADE
);

CREATE TABLE Events(
  event_id INT,
  event_location VARCHAR(80),
  event_date DATE,
  PRIMARY KEY (event_id)
);