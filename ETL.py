

# Import necessary libraries

import pandas as pd # Data manipulation and analysis
import numpy as np # Numerical operations
from sqlalchemy import create_engine, text # SQL toolkit and Object-Relational Mapping (ORM)
from sqlalchemy import insert
import re # Regular expressions for string manipulation
import statistics

engine = create_engine("postgresql+psycopg2://postgres:d4tAg3n1Us$+_*@localhost:5432/UFC Data Analysis")

#Helper functions that are used for cleaning and formatting data
def clean_fighter_names(name):
    if pd.isna(name):
        return name
    
    name = name.strip().title()  # Remove leading/trailing spaces and capitalize
    
    name = re.sub(r'\s+', ' ', name)  # Replace multiple spaces with a single space
    
    return name

def create_ids(df):
    df = df.copy()
    df['id'] = range(1, len(df) + 1)
    return df

def time_parser(time):
    if(isinstance(time, float)):
        return
    minutes = time[0:1]
    seconds = (int(minutes)*60) + (int(time[2:]))
    return seconds

# Functions to format the dataframe column names to their respective PostgreSQL table attributes
def create_fighter_table(df):
    red_fighters = df[['RedFighter', 'RedHeightCms', 'RedReachCms', 'RedStance', 'Gender']].rename(columns={
        'RedFighter': 'fighter_name', 
        'RedHeightCms': 'height_cms',
        'RedReachCms': 'reach_cms',
        'RedStance': 'stance',
        'Gender': 'gender'
        })

    blue_fighters = df[['BlueFighter', 'BlueHeightCms', 'BlueReachCms', 'BlueStance', 'Gender']].rename(columns={
        'BlueFighter': 'fighter_name', 
        'BlueHeightCms': 'height_cms',
        'BlueReachCms': 'reach_cms',
        'BlueStance': 'stance',
        'Gender': 'gender'
    })

    all_fighters = pd.concat([red_fighters, blue_fighters], ignore_index=True)
    all_fighters['fighter_name'] = all_fighters['fighter_name'].apply(clean_fighter_names)

    all_fighters = all_fighters.dropna(subset=['fighter_name'])
    
    unique_fighters = all_fighters.groupby('fighter_name').agg(
        stance=('stance', lambda x: x.value_counts().index[0]),
        reach_cms=('reach_cms', 'mean'),
        height_cms=('height_cms', 'mean'),
        gender=('gender', 'first')
    ).reset_index()

    unique_fighters['reach_cms'] = unique_fighters['reach_cms'].round(2)
    unique_fighters['height_cms'] = unique_fighters['height_cms'].round(2)

    unique_fighters = create_ids(unique_fighters)
    unique_fighters.rename(columns={'id': 'fighter_id'}, inplace=True)

    unique_fighters = unique_fighters.astype({
        'fighter_name': 'str',
        'stance': 'str',
        'gender': 'str'
    })
    
    return unique_fighters

def create_event_table(df):
    events = df[['Date', 'Location']].rename(columns={
        'Date': 'event_date',
        'Location': 'event_location',
    })
    
    unique_events = events.drop_duplicates()
    unique_events = create_ids(unique_events)
    unique_events = unique_events.rename(columns={'id': 'event_id'})
    
    unique_events = unique_events.astype({
        'event_location': 'str'
    })
    
    return unique_events

def create_fight_table(df, fighters, events):
    all_fights = df[['TitleBout', 'NumberOfRounds', 'Winner', 'WeightClass', 'Finish', 'FinishDetails', 'FinishRound', 'FinishRoundTime',
                          'TotalFightTimeSecs']].rename(columns={
        'TitleBout': 'title_bout',
        'NumberOfRounds': 'num_rounds',
        'Winner': 'winner_color',
        'WeightClass': 'weight_class', 
        'Finish': 'finish_method', 
        'FinishDetails': 'finish_details', 
        'FinishRound': 'finish_round', 
        'FinishRoundTime': 'finish_round_time', 
        'TotalFightTimeSecs': 'total_fight_time_seconds',
    })
    
    fighter_mapping = dict(zip(fighters['fighter_name'], fighters['fighter_id']))
    event_mapping = dict(zip(events[['event_date', 'event_location']].apply(tuple, axis = 1), events['event_id']))
    all_fights['red_fighter_id'] = df['RedFighter'].apply(clean_fighter_names).map(fighter_mapping)
    all_fights['blue_fighter_id'] = df['BlueFighter'].apply(clean_fighter_names).map(fighter_mapping)
    temp_event_tuple = df[['Date', 'Location']].apply(tuple, axis = 1)
    all_fights['event_id'] = temp_event_tuple.map(event_mapping)

    unique_fights = all_fights.drop_duplicates()
    unique_fights = create_ids(all_fights)
    unique_fights.rename(columns={'id': 'fight_id'}, inplace=True)
    unique_fights['finish_round_time'] = unique_fights['finish_round_time'].apply(time_parser)
    
    unique_fights = unique_fights.astype({
        'winner_color': 'str',
        'weight_class': 'str',
        'finish_method': 'str',
        'finish_details': 'str',
        'finish_round': 'Int64',
        'finish_round_time': 'Int64',
        'total_fight_time_seconds': 'Int64'
    })
    
    return unique_fights

def create_fighter_stats_per_fight_table(df, fighters):
    red_fighters = df[['RedWeightLbs', 'RedAge', 'RedCurrentWinStreak', 'RedCurrentLoseStreak', 
                       'RedLongestWinStreak', 'RedWins', 'RedLosses', 'RedDraws', 'RedWinsByKO', 'RedWinsBySubmission',
                       'RedWinsByTKODoctorStoppage', 'RedWinsByDecisionUnanimous', 'RedWinsByDecisionMajority',
                       'RedWinsByDecisionSplit', 'RedAvgSigStrLanded', 'RedAvgSigStrPct', 'RedAvgSubAtt', 'RedAvgTDLanded',
                       'RedAvgTDPct'
                       ]].rename(columns={
        'RedWeightLbs': 'weight_lbs', 
        'RedAge': 'age', 
        'RedCurrentWinStreak': 'current_win_streak', 
        'RedCurrentLoseStreak': 'current_lose_streak', 
        'RedLongestWinStreak': 'longest_win_streak',
        'RedWins': 'total_wins',
        'RedLosses': 'total_losses',
        'RedDraws': 'total_draws',
        'RedWinsByKO': 'wins_by_ko',
        'RedWinsBySubmission': 'wins_by_submission',
        'RedWinsByTKODoctorStoppage': 'wins_by_tko_doctor_stoppage',
        'RedWinsByDecisionUnanimous': 'wins_by_decision_unanimous',
        'RedWinsByDecisionMajority': 'wins_by_decision_majority',
        'RedWinsByDecisionSplit': 'wins_by_decision_split',
        'RedAvgSigStrLanded': 'avg_sig_strikes_landed',
        'RedAvgSigStrPct': 'avg_sig_strikes_pct',
        'RedAvgSubAtt': 'avg_submission_attempts',
        'RedAvgTDLanded': 'avg_takedowns_landed',
        'RedAvgTDPct': 'avg_takedowns_pct'
    })
                       
    blue_fighters = df[['BlueWeightLbs', 'BlueAge', 'BlueCurrentWinStreak', 'BlueCurrentLoseStreak', 
                        'BlueLongestWinStreak', 'BlueWins', 'BlueLosses', 'BlueDraws', 'BlueWinsByKO', 'BlueWinsBySubmission',
                        'BlueWinsByTKODoctorStoppage', 'BlueWinsByDecisionUnanimous', 'BlueWinsByDecisionMajority',
                        'BlueWinsByDecisionSplit', 'BlueAvgSigStrLanded', 'BlueAvgSigStrPct', 'BlueAvgSubAtt', 'BlueAvgTDLanded',
                        'BlueAvgTDPct'
                   ]].rename(columns={
        'BlueWeightLbs': 'weight_lbs', 
        'BlueAge': 'age', 
        'BlueCurrentWinStreak': 'current_win_streak', 
        'BlueCurrentLoseStreak': 'current_lose_streak', 
        'BlueLongestWinStreak': 'longest_win_streak',
        'BlueWins': 'total_wins',
        'BlueLosses': 'total_losses',
        'BlueDraws': 'total_draws',
        'BlueWinsByKO': 'wins_by_ko',
        'BlueWinsBySubmission': 'wins_by_submission',
        'BlueWinsByTKODoctorStoppage': 'wins_by_tko_doctor_stoppage',
        'BlueWinsByDecisionUnanimous': 'wins_by_decision_unanimous',
        'BlueWinsByDecisionMajority': 'wins_by_decision_majority',
        'BlueWinsByDecisionSplit': 'wins_by_decision_split',
        'BlueAvgSigStrLanded': 'avg_sig_strikes_landed',
        'BlueAvgSigStrPct': 'avg_sig_strikes_pct',
        'BlueAvgSubAtt': 'avg_submission_attempts',
        'BlueAvgTDLanded': 'avg_takedowns_landed',
        'BlueAvgTDPct': 'avg_takedowns_pct'
    })
                   
    red_fighters['fighter_corner'] = 'Red'
    blue_fighters['fighter_corner'] = 'Blue'
    
    full_red_fighters = create_ids(red_fighters)
    full_blue_fighters = create_ids(blue_fighters)
    
    full_red_fighters.rename(columns={'id': 'fight_id'}, inplace=True)
    full_blue_fighters.rename(columns={'id': 'fight_id'}, inplace=True)
    
    fighter_mapping = dict(zip(fighters['fighter_name'], fighters['fighter_id']))
    full_red_fighters['fighter_id'] = df['RedFighter'].apply(clean_fighter_names).map(fighter_mapping)
    full_blue_fighters['fighter_id'] = df['BlueFighter'].apply(clean_fighter_names).map(fighter_mapping)
    
    all_fighters_stats = pd.concat([full_red_fighters, full_blue_fighters], ignore_index=True)
    
    fighter_stats = create_ids(all_fighters_stats)
    fighter_stats.rename(columns={'id': 'stat_id'}, inplace=True)
    
    fighter_stats['avg_sig_strikes_landed'] = fighter_stats['avg_sig_strikes_landed'].round(2)
    fighter_stats['avg_sig_strikes_pct'] = fighter_stats['avg_sig_strikes_pct'].round(2)
    fighter_stats['avg_submission_attempts'] = fighter_stats['avg_submission_attempts'].round(2)
    fighter_stats['avg_takedowns_landed'] = fighter_stats['avg_takedowns_landed'].round(2)
    fighter_stats['avg_takedowns_pct'] = fighter_stats['avg_takedowns_pct'].round(2)
    
    fighter_stats = fighter_stats.astype({
        'fighter_corner': 'str'
    })
    
    return fighter_stats

def create_betting_odds_table(df):
    betting_odds = df[['RedOdds', 'RedExpectedValue', 'RedDecOdds', 'RSubOdds','RKOOdds', 
                       'BlueOdds', 'BlueExpectedValue', 'BlueDecOdds', 'BSubOdds', 'BKOOdds']].rename(columns ={
        'RedOdds': 'red_odds', 
        'RedExpectedValue': 'red_expected_value', 
        'RedDecOdds': 'red_dec_odds', 
        'RSubOdds': 'red_submission_odds',
        'RKOOdds': 'red_ko_odds', 
        'BlueOdds': 'blue_odds', 
        'BlueExpectedValue': 'blue_expected_value', 
        'BlueDecOdds': 'blue_dec_odds', 
        'BSubOdds': 'blue_submission_odds',
        'BKOOdds': 'blue_ko_odds'
     })
    
    betting_odds = create_ids(betting_odds)
    betting_odds.rename(columns={'id': 'odds_id'}, inplace=True)
    
    betting_odds = create_ids(betting_odds)
    betting_odds.rename(columns={'id': 'fight_id'}, inplace=True)
    
    betting_odds['red_odds'] = betting_odds['red_odds'].round(2)
    betting_odds['red_expected_value'] = betting_odds['red_expected_value'].round(4)
    betting_odds['red_dec_odds'] = betting_odds['red_dec_odds'].round(2)
    betting_odds['red_submission_odds'] = betting_odds['red_submission_odds'].round(2)
    betting_odds['red_ko_odds'] = betting_odds['red_ko_odds'].round(2)
    betting_odds['blue_odds'] = betting_odds['blue_odds'].round(2)
    betting_odds['blue_expected_value'] = betting_odds['blue_expected_value'].round(4)
    betting_odds['blue_dec_odds'] = betting_odds['blue_dec_odds'].round(2)
    betting_odds['blue_submission_odds'] = betting_odds['blue_submission_odds'].round(2)
    betting_odds['blue_ko_odds'] = betting_odds['blue_ko_odds'].round(2)
    
    return betting_odds

def create_fighter_rankings(df, fighters):
    red_fighter_rankings = df[['RMatchWCRank', 'RWFlyweightRank', 'RWFeatherweightRank',
                           'RWStrawweightRank', 'RWBantamweightRank', 'RHeavyweightRank',
                           'RLightHeavyweightRank', 'RMiddleweightRank', 'RWelterweightRank',
                           'RLightweightRank', 'RFeatherweightRank', 'RBantamweightRank',
                           'RFlyweightRank', 'RPFPRank']].rename(columns={
        'RPFPRank': 'pfp_rank',
        'RMatchWCRank': 'weight_class_rank',
        'RFlyweightRank': 'flyweight_rank',
        'RBantamweightRank': 'bantamweight_rank',
        'RFeatherweightRank': 'featherweight_rank',
        'RLightweightRank': 'lightweight_rank',
        'RWelterweightRank': 'welterweight_rank',
        'RMiddleweightRank': 'middleweight_rank',
        'RLightHeavyweightRank': 'light_heavyweight_rank',
        'RHeavyweightRank': 'heavyweight_rank',
        'RWStrawweightRank': 'w_strawweight_rank',
        'RWFlyweightRank': 'w_flyweight_rank',
        'RWBantamweightRank': 'w_bantamweight_rank',
        'RWFeatherweightRank': 'w_featherweight_rank'
    })
                           
    blue_fighter_rankings = df[['BMatchWCRank', 'BWFlyweightRank', 'BWFeatherweightRank',
                           'BWStrawweightRank', 'BWBantamweightRank', 'BHeavyweightRank',
                           'BLightHeavyweightRank', 'BMiddleweightRank', 'BWelterweightRank',
                           'BLightweightRank', 'BFeatherweightRank', 'BBantamweightRank',
                           'BFlyweightRank','BPFPRank']].rename(columns={
        'BPFPRank': 'pfp_rank',
        'BMatchWCRank': 'weight_class_rank',
        'BFlyweightRank': 'flyweight_rank',
        'BBantamweightRank': 'bantamweight_rank',
        'BFeatherweightRank': 'featherweight_rank',
        'BLightweightRank': 'lightweight_rank',
        'BWelterweightRank': 'welterweight_rank',
        'BMiddleweightRank': 'middleweight_rank',
        'BLightHeavyweightRank': 'light_heavyweight_rank',
        'BHeavyweightRank': 'heavyweight_rank',
        'BWStrawweightRank': 'w_strawweight_rank',
        'BWFlyweightRank': 'w_flyweight_rank',
        'BWBantamweightRank': 'w_bantamweight_rank',
        'BWFeatherweightRank': 'w_featherweight_rank'
    })
    
    red_fighter_rankings['corner_color'] = 'Red'
    blue_fighter_rankings['corner_color'] = 'Blue'
    
    full_red_fighters_rankings = create_ids(red_fighter_rankings)
    full_blue_fighters_rankings = create_ids(blue_fighter_rankings)
    
    full_red_fighters_rankings.rename(columns={'id': 'fight_id'}, inplace=True)
    full_blue_fighters_rankings.rename(columns={'id': 'fight_id'}, inplace=True)
    
    full_red_fighters_rankings['better_rank'] = None
    full_blue_fighters_rankings['better_rank'] = None
    
    red_count = 1
    blue_count = 1
    row_indexer = 0
    
    for i in df['BetterRank']:
        if(i == 'Red'):
            full_red_fighters_rankings.loc[row_indexer, 'better_rank'] = True
            full_blue_fighters_rankings.loc[row_indexer, 'better_rank'] = False
            red_count += 1
            row_indexer += 1
        elif(i == 'Blue'):
            full_red_fighters_rankings.loc[row_indexer, 'better_rank'] = False
            full_blue_fighters_rankings.loc[row_indexer, 'better_rank'] = True
            blue_count += 1
            row_indexer += 1
        else:
            full_red_fighters_rankings.loc[row_indexer, 'better_rank'] = None
            full_blue_fighters_rankings.loc[row_indexer, 'better_rank'] = None
            red_count += 1
            blue_count += 1
            row_indexer += 1
    
    fighter_mapping = dict(zip(fighters['fighter_name'], fighters['fighter_id']))
    full_red_fighters_rankings['fighter_id'] = df['RedFighter'].apply(clean_fighter_names).map(fighter_mapping)
    full_blue_fighters_rankings['fighter_id'] = df['BlueFighter'].apply(clean_fighter_names).map(fighter_mapping)
    
    all_fighter_ranks = pd.concat([full_red_fighters_rankings, full_blue_fighters_rankings], ignore_index=True)
    
    fighter_ranks = create_ids(all_fighter_ranks)
    fighter_ranks.rename(columns={'id': 'ranking_id'}, inplace=True)
    
    fighter_ranks = fighter_ranks.astype({
        'weight_class_rank': 'Int64',
        'w_flyweight_rank': 'Int64',
        'w_featherweight_rank': 'Int64',
        'w_strawweight_rank': 'Int64',
        'w_bantamweight_rank': 'Int64',
        'heavyweight_rank': 'Int64',
        'light_heavyweight_rank': 'Int64',
        'middleweight_rank': 'Int64',
        'welterweight_rank': 'Int64',
        'lightweight_rank': 'Int64',
        'featherweight_rank': 'Int64',
        'bantamweight_rank': 'Int64',
        'flyweight_rank': 'Int64',
        'pfp_rank': 'Int64',
        'corner_color': 'str',
        'better_rank': 'bool'
    })
    
    return fighter_ranks

def create_fight_differentials(df):
    df = df.replace('', np.nan)
    
    df['RedHeightCms'] = pd.to_numeric(df['RedHeightCms'], errors='coerce')
    df['BlueHeightCms'] = pd.to_numeric(df['BlueHeightCms'], errors='coerce')
    df['RedReachCms'] = pd.to_numeric(df['RedReachCms'], errors='coerce')
    df['BlueReachCms'] = pd.to_numeric(df['BlueReachCms'], errors='coerce')
    df['RedAvgSigStrLanded'] = pd.to_numeric(df['RedAvgSigStrLanded'], errors='coerce')
    df['BlueAvgSigStrLanded'] = pd.to_numeric(df['BlueAvgSigStrLanded'], errors='coerce')
    df['RedAvgSubAtt'] = pd.to_numeric(df['RedAvgSubAtt'], errors='coerce')
    df['BlueAvgSubAtt'] = pd.to_numeric(df['BlueAvgSubAtt'], errors='coerce')
    df['RedAvgTDLanded'] = pd.to_numeric(df['RedAvgTDLanded'], errors='coerce')
    df['BlueAvgTDLanded'] = pd.to_numeric(df['BlueAvgTDLanded'], errors='coerce')
    
    lose_streak_diff = (df['RedCurrentLoseStreak'] - df['BlueCurrentLoseStreak'])
    win_streak_diff = df['RedCurrentWinStreak'] - df['BlueCurrentWinStreak']
    longest_win_streak_diff = df['RedLongestWinStreak'] - df['BlueLongestWinStreak']
    wins_diff = df['RedWins'] - df['BlueWins']
    losses_diff = df['RedLosses'] - df['BlueLosses']
    draws_diff = df['RedDraws'] - df['BlueDraws']
    total_rounds_diff = df['RedTotalRoundsFought'] - df['BlueTotalRoundsFought']
    total_title_bouts_diff = df['RedTotalTitleBouts'] - df['BlueTotalTitleBouts']
    KO_diff = df['RedWinsByKO'] - df['BlueWinsByKO']
    submission_diff = df['RedWinsBySubmission'] - df['BlueWinsBySubmission']
    height_cms_diff = df['RedHeightCms'] - df['BlueHeightCms']
    reach_cms_diff = df['RedReachCms'] - df['BlueReachCms']
    weight_lbs_diff = df['RedWeightLbs'] - df['BlueWeightLbs']
    age_diff = df['RedAge'] - df['BlueAge']
    sig_strikes_diff = df['RedAvgSigStrLanded'] - df['BlueAvgSigStrLanded']
    avg_submission_att_diff = df['RedAvgSubAtt'] - df['BlueAvgSubAtt']
    avg_takedown_landed_diff = df['RedAvgTDLanded'] - df['BlueAvgTDLanded']
        
    all_fighters_differentials = pd.DataFrame({'lose_streak_diff': lose_streak_diff, 'win_streak_diff': win_streak_diff,'longest_win_streak_diff': longest_win_streak_diff,'wins_diff' : wins_diff,
                                              'losses_diff': losses_diff, 'draws_diff': draws_diff, 'total_rounds_diff': total_rounds_diff, 'total_title_bouts_diff' : total_title_bouts_diff,
                                              'ko_diff': KO_diff, 'submission_diff': submission_diff, 'height_cms_diff': height_cms_diff, 'reach_cms_diff': reach_cms_diff, 'weight_lbs_diff': weight_lbs_diff,
                                              'age_diff': age_diff, 'sig_strikes_diff': sig_strikes_diff, 'avg_submission_att_diff': avg_submission_att_diff, 'avg_takedown_landed_diff': avg_takedown_landed_diff})
    fighters_differentials = create_ids(all_fighters_differentials)
    fighters_differentials.rename(columns={'id': 'fight_id'}, inplace=True)
    
    unique_fighters_differentials = create_ids(fighters_differentials)
    unique_fighters_differentials.rename(columns={'id': 'differential_id'}, inplace=True)
    
    unique_fighters_differentials['height_cms_diff'] = unique_fighters_differentials['height_cms_diff'].round(2)
    unique_fighters_differentials['reach_cms_diff'] = unique_fighters_differentials['reach_cms_diff'].round(2)
    unique_fighters_differentials['sig_strikes_diff'] = unique_fighters_differentials['sig_strikes_diff'].round(2)
    unique_fighters_differentials['avg_submission_att_diff'] = unique_fighters_differentials['avg_submission_att_diff'].round(2)
    unique_fighters_differentials['avg_takedown_landed_diff'] = unique_fighters_differentials['avg_takedown_landed_diff'].round(2)

    
    return unique_fighters_differentials
    
df = pd.read_csv('ufc-master.csv', na_values=[''])

all_fighters = create_fighter_table(df)

all_events = create_event_table(df)

all_fights = create_fight_table(df, all_fighters, all_events)

all_stats = create_fighter_stats_per_fight_table(df, all_fighters)

all_odds = create_betting_odds_table(df)

all_fighter_rankings = create_fighter_rankings(df, all_fighters)

all_differentials = create_fight_differentials(df)

all_fighters.to_sql(name='fighters', con=engine, index=False, if_exists='append', method='multi')
all_events.to_sql(name='events', con=engine,index=False, if_exists='append', method='multi')
all_fights.to_sql(name='fights', con=engine,index=False, if_exists='append', method='multi')
all_stats.to_sql(name='fighter_stats_per_fight', con=engine,index=False, if_exists='append', method='multi')
all_odds.to_sql(name='betting_odds', con=engine,index=False, if_exists='append', method='multi')
all_fighter_rankings.to_sql(name='fighter_rankings', con=engine,index=False, if_exists='append', method='multi')
all_differentials.to_sql(name="fight_differentials", con=engine,index=False, if_exists='append', method='multi')

with engine.connect() as conn:
    conn.execute(text("SELECT * FROM fighters")).fetchall()
    conn.commit()
