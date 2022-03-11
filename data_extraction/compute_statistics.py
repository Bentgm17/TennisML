from re import S
from unicodedata import name
import pandas as pd
from sqlalchemy import column
from docker_extract import ExtractData
import os
import numpy as np
from tqdm import tqdm
import sys
import requests
from bs4 import BeautifulSoup

class Transformation():
    """
    A class used to get transform the dataframe into statistics representing a match

    ...

    Attributes
    ----------
    df : pd.DataFrame
       Dataframe with the matches as entry
    new_df : pd.DataFrame
       target df with all stats of the match
    new_df_cond : list
       Conditions for columns in new_df to filter out wrong entries

    Methods
    -------
    __init__()
        Initializes the class
    __call__()
        Runs all necessary functions to compute statistics
    add_percentage_condition()
        Adds the condition to the new dataframe condition list. Condition is : 0<x<1
    copy_match_and_playerid()
        Copies old dataframe's match_id,winner_id,loser_id and date to new df
    compute_ace()
        Computation of the ace percentage of the winner and loser
    compute_df()
        Computation of the double fault percentage of the winner and loser

    """

    def __init__(self,df):
        """
        Initializes the class
        Parameters
        ----------
        df:pd.Dataframe
            The dataframe with entries of all matches
        """
        self.df=df
        self.new_df=pd.DataFrame([])
        self.new_df_cond=[]
        self.extract_data=ExtractData()
    
    def __call__(self):
        """
        Runs all necessary functions to compute statistics
        """
        self.correct_wrong_rows()
        self.copy_match_and_playerid()
        self.compute_elo_ranking_surface_indoor()
        self.compute_ace()
        self.compute_df(),self.compute_total_points(),self.compute_points_won_on_serve()
        self.compute_points_won_on_first_serve(),self.compute_points_won_on_second_serve(),self.compute_break_points_saved()
        self.compute_return_points_won_on_return_first_serve(),self.compute_return_points_won_on_return_second_serve()
        self.compute_break_points_won(),self.compute_return_points_won(),self.compute_point_dominance()
        self.compute_numbers_of_first_serve_made(),self.compute_aces_per_game()
        self.compute_df_per_game(),self.compute_aces_per_set(),self.compute_df_per_set()
        self.compute_df_per_second_serve(),self.compute_aces_per_df(),self.compute_first_serve_efficiency()
        self.compute_serve_ip_efficiency(),self.compute_points_lost_per_serv_game(),self.compute_break_points_faced_per_game()
        self.compute_break_points_faced_per_set(),self.compute_service_games_won(),self.compute_sv_games_lost_per_set()
        self.compute_ip_return_points(),self.compute_points_won_per_return_game(),self.compute_bp_per_game()
        self.compute_bp_per_set(),self.compute_return_games_won(),self.compute_return_games_won_per_set()
        self.compute_games_won(),self.compute_game_dominance(),self.compute_break_ratio(),self.compute_winning_percentage(),self.clean(),self.get_odds()

    
    def add_percentage_condition(self,column1,column2):
        """
        Adds the condition to the new dataframe condition list. Condition is : 0<x<1

        Parameters
        ----------
        column1:
            Winners version of certain statistic
        column2:
            Losers version of certain statistic

        """
        self.new_df_cond.append(self.new_df[column1].between(0, 1))
        self.new_df_cond.append(self.new_df[column2].between(0, 1))

    def copy_match_and_playerid(self):
        """
        Copies old dataframe's match_id,winner_id,loser_id and date to new df.
        """
        self.new_df['match_id']=self.df['match_id']
        self.new_df['winner_id']=self.df['winner_id']
        self.new_df['loser_id']=self.df['loser_id']
        self.new_df['w_rank'],self.new_df['l_rank']=self.df['winner_rank'],self.df['loser_rank']
        self.new_df['w_elo_r'],self.new_df['l_elo_r']=self.df['winner_elo_rating'],self.df['loser_elo_rating']
        self.new_df['date']=pd.to_datetime(self.df['date'])

    def compute_ace(self):
        '''
        Computation of the ace percentage of the winner and loser
        '''
        self.new_df['w_ace'],self.new_df['l_ace']=self.df['w_ace']/self.df['w_sv_pt'],self.df['l_ace']/self.df['l_sv_pt']
        self.add_percentage_condition('w_ace','l_ace')

    def compute_df(self):
        '''
        Computation of the double fault percentage of the winner and loser
        '''
        self.new_df['w_df'],self.new_df['l_df']=self.df['w_df']/self.df['w_sv_pt'],self.df['l_df']/self.df['l_sv_pt']
        self.add_percentage_condition('w_df','l_df')

    def compute_total_points(self):
        '''
        Computation of the total points percentage of the winner and loser
        '''
        self.new_df['w_tt_p']=((self.df['w_1st_won']+self.df['w_2nd_won'])+(self.df['l_sv_pt']-(self.df['l_1st_won']+self.df['l_2nd_won'])))/(self.df['l_sv_pt']+self.df['w_sv_pt'])
        self.new_df['l_tt_p']=((self.df['l_1st_won']+self.df['l_2nd_won'])+(self.df['w_sv_pt']-(self.df['w_1st_won']+self.df['w_2nd_won'])))/(self.df['w_sv_pt']+self.df['l_sv_pt'])
        self.add_percentage_condition('w_tt_p','l_tt_p')

    def compute_points_won_on_serve(self):
        '''
        Computation of the points won on serve percentage of the winner and loser
        '''
        self.new_df['w_sv_w'],self.new_df['l_sv_w']=(self.df['w_1st_won']+self.df['w_2nd_won'])/self.df['w_sv_pt'],(self.df['l_1st_won']+self.df['l_2nd_won'])/self.df['l_sv_pt']
        self.add_percentage_condition('w_sv_w','l_sv_w')

    def compute_points_won_on_first_serve(self):
        '''
        Computation of the points won on first serve percentage of the winner and loser
        '''
        self.new_df['w_1st_w'],self.new_df['l_1st_w']=self.df['w_1st_won']/self.df['w_1st_in'],self.df['l_1st_won']/self.df['l_1st_in']
        self.add_percentage_condition('w_1st_w','l_1st_w')

    def compute_points_won_on_second_serve(self):
        '''
        Computation of the points won on second serve percentage of the winner and loser
        '''
        self.new_df['w_2nd_w'],self.new_df['l_2nd_w']=self.df['w_2nd_won']/(self.df['w_sv_pt']-self.df['w_1st_in']),self.df['l_2nd_won']/(self.df['l_sv_pt']-self.df['l_1st_in'])
        self.add_percentage_condition('w_2nd_w','l_2nd_w')

    def compute_break_points_saved(self):
        '''
        Computation of the breakpoints saved percentage of the winner and loser
        '''
        self.new_df['w_bp_save'],self.new_df['l_bp_save']=(self.df['w_bp_sv']/self.df['w_bp_fc']).fillna(0),(self.df['l_bp_sv']/self.df['l_bp_fc']).fillna(0)
        self.add_percentage_condition('w_bp_save','l_bp_save')

    def compute_return_points_won_on_return_first_serve(self):
        '''
        Computation of the won on first return serve percentage of the winner and loser
        '''
        self.new_df['w_r_1st_w'],self.new_df['l_r_1st_w']=1-self.new_df['l_1st_w'],1-self.new_df['w_1st_w']
        self.add_percentage_condition('w_r_1st_w','l_r_1st_w')

    def compute_return_points_won_on_return_second_serve(self):
        '''
        Computation of the won on second return serve percentage of the winner and loser
        '''
        self.new_df['w_r_1st_w'],self.new_df['l_r_1st_w']=1-self.new_df['l_1st_w'],1-self.new_df['w_1st_w']
        self.add_percentage_condition('w_r_1st_w','l_r_1st_w')

    def compute_break_points_won(self):
        '''
        Computation of break points won percentage of the winner and loser
        '''
        self.new_df['w_bp_w'],self.new_df['l_bp_w']=1-self.new_df['l_bp_save'],1-self.new_df['w_bp_save']
        self.add_percentage_condition('w_bp_w','l_bp_w')

    def compute_return_points_won(self):
        '''
        Computation of return points won percentage of the winner and loser
        '''
        self.new_df['w_rp_w'],self.new_df['l_rp_w']=1-self.new_df['l_sv_w'],1-self.new_df['w_sv_w']
        self.add_percentage_condition('w_rp_w','l_rp_w')

    def compute_point_dominance(self):
        '''
        Computation of points_dominance of the winner and loser
        point_dominance--> return points won/return points won opponent
        '''
        self.new_df['w_p_dominance'],self.new_df['l_p_dominance']=self.new_df['w_rp_w']/self.new_df['l_rp_w'],self.new_df['l_rp_w']/self.new_df['w_rp_w']

    def compute_numbers_of_first_serve_made(self):
        '''
        Computation of the percentage of first serves of the winner and loser
        '''
        self.new_df['w_1st_in'],self.new_df['l_1st_in']=self.df['w_1st_in']/self.df['w_sv_pt'],self.df['l_1st_in']/self.df['l_sv_pt']
        self.add_percentage_condition('w_1st_in','l_1st_in')

    def compute_aces_per_game(self):
        '''
        Computation of the aces per game of the winner and loser
        '''
        self.new_df['w_ace_game'],self.new_df['l_ace_game']=self.df['w_ace']/self.df['w_sv_gms'],self.df['l_ace']/self.df['l_sv_gms']

    def compute_df_per_game(self):
        '''
        Computation of the double faults per game of the winner and loser
        '''
        self.new_df['w_df_game'],self.new_df['l_df_game']=self.df['w_df']/self.df['w_sv_gms'],self.df['l_df']/self.df['l_sv_gms']

    def compute_aces_per_set(self):
        '''
        Computation of the aces per set of the winner and loser
        '''
        self.new_df['w_ace_set'],self.new_df['l_ace_set']=self.df['w_ace']/(self.df['w_sets']+self.df['l_sets']),self.df['l_ace']/(self.df['w_sets']+self.df['l_sets'])

    def compute_df_per_set(self):
        '''
        Computation of the double faults per set of the winner and loser
        '''
        self.new_df['w_df_set'],self.new_df['l_df_set']=self.df['w_df']/(self.df['w_sets']+self.df['l_sets']),self.df['l_df']/(self.df['w_sets']+self.df['l_sets'])

    def compute_df_per_second_serve(self):
        '''
        Computation of the double faults per second serve of the winner and loser
        '''
        self.new_df['w_df_2nd_sv'],self.new_df['l_df_2nd_sv']=self.df['w_df']/(self.df['w_sv_pt']-self.df['w_1st_in']),self.df['l_df']/(self.df['l_sv_pt']-self.df['l_1st_in'])
        self.add_percentage_condition('w_df_2nd_sv','l_df_2nd_sv')
        
    def compute_aces_per_df(self):
        '''
        Computation of the aces per double daults of the winner and loser
        '''
        self.new_df['w_ace_df_ratio'],self.new_df['l_ace_df_ratio']=(self.new_df['w_ace'] / self.new_df['w_df']).replace((np.nan,np.inf, -np.inf), (0,0,0)),(self.new_df['l_ace'] / self.new_df['l_df']).replace((np.nan,np.inf, -np.inf), (0,0,0))

    def compute_first_serve_efficiency(self):
        '''
        Computation of the first serve efficiency of the winner and loser
        first serve efficiency : The points won on first serve(%)/ points won on second serve (%)
        '''
        self.new_df['w_1st_sv_eff'],self.new_df['l_1st_sv_eff']=self.new_df['w_1st_w']/self.new_df['w_2nd_w'],self.new_df['l_1st_w']/self.new_df['l_2nd_w']

    def compute_serve_ip_efficiency(self):
        '''
        Computation of serve in-play efficiency of the winner and loser
        serve in-play efficiency: points won on serve - aces - double faults
        '''
        self.new_df['w_sv_ip_w'],self.new_df['l_sv_ip_w']=(self.df['w_1st_won']+self.df['w_2nd_won']-self.df['w_ace']-self.df['w_df'])/(self.df['w_sv_pt']-self.df['w_ace']-self.df['w_df']),(self.df['l_1st_won']+self.df['l_2nd_won']-self.df['l_ace']-self.df['l_df'])/(self.df['l_sv_pt']-self.df['l_ace']-self.df['l_df'])
        self.add_percentage_condition('w_sv_ip_w','l_sv_ip_w')

    def compute_points_lost_per_serv_game(self):
        '''
        Computation of the points lost per service game of the winner and loser
        '''
        self.new_df['w_ploss_game'],self.new_df['l_ploss_game']=(self.df['w_sv_pt']-self.df['w_1st_won']-self.df['w_2nd_won'])/self.df['w_sv_gms'],(self.df['l_sv_pt']-self.df['l_1st_won']-self.df['l_2nd_won'])/self.df['l_sv_gms']

    def compute_break_points_faced_per_game(self):
        '''
        Computation of the breakpoints faced per game of the winner and loser
        '''
        self.new_df['w_bpf_game'],self.new_df['l_bpf_game']=self.df['w_bp_fc']/self.df['w_sv_gms'],self.df['l_bp_fc']/self.df['l_sv_gms']

    def compute_break_points_faced_per_set(self):
        '''
        Computation of the breakpoints faced per set of the winner and loser
        '''
        self.new_df['w_bpf_set'],self.new_df['l_bpf_set']=self.df['w_bp_fc']/(self.df['w_sets']+self.df['l_sets']),self.df['l_bp_fc']/(self.df['w_sets']+self.df['l_sets'])

    def compute_service_games_won(self):
        '''
        Computation of the service game won percentage of the winner and loser
        '''
        self.new_df['w_svp_game'],self.new_df['l_svp_game']=1-(self.df['w_bp_fc']-self.df['w_bp_sv'])/self.df['w_sv_gms'],1-(self.df['l_bp_fc']-self.df['l_bp_sv'])/self.df['l_sv_gms']
        self.add_percentage_condition('w_svp_game','l_svp_game')

    def compute_sv_games_lost_per_set(self):
        '''
        Computation of the service game lost per set percentage of the winner and loser
        '''
        self.new_df['w_sv_gm_lset'],self.new_df['l_sv_gm_lset']=(self.df['w_bp_fc']-self.df['w_bp_sv'])/(self.df['w_sets']+self.df['l_sets']),(self.df['l_bp_fc']-self.df['l_bp_sv'])/(self.df['w_sets']+self.df['l_sets'])

    def compute_ip_return_points(self):
        '''
        Computation of the in-play return points of the winner and loser
        return in-play: points won on return - return aces - return double faults
        '''
        self.new_df['w_r_ip_w'],self.new_df['l_r_ip_w']=1-self.new_df['l_sv_ip_w'],1-self.new_df['w_sv_ip_w']
        self.add_percentage_condition('w_r_ip_w','l_r_ip_w')

    def compute_points_won_per_return_game(self):
        '''
        Computation of the points won per return game of the winner and loser
        '''
        self.new_df['w_pwon_r_game'],self.new_df['l_pwon_r_game']=self.new_df['l_ploss_game'],self.new_df['w_ploss_game']

    def compute_bp_per_game(self):
        '''
        Computation of the double faults per game of the winner and loser
        '''
        self.new_df['w_bp_game'],self.new_df['l_bp_game']=self.new_df['l_bpf_game'],self.new_df['w_bpf_game']

    def compute_bp_per_set(self):
        '''
        Computation of the double faults per set of the winner and loser
        '''
        self.new_df['w_bp_set'],self.new_df['l_bp_set']=self.new_df['l_bpf_set'],self.new_df['w_bpf_set']

    def compute_return_games_won(self):
        '''
        Computation of the games won on return games of the winner and loser
        '''
        self.new_df['w_r_game_w'],self.new_df['l_r_game_w']=1-self.new_df['l_svp_game'],1-self.new_df['w_svp_game']
        self.add_percentage_condition('w_r_game_w','l_r_game_w')

    def compute_return_games_won_per_set(self):
        '''
        Computation of the games won on return games per set of the winner and loser
        '''
        self.new_df['w_r_gm_w_set'],self.new_df['l_r_gm_w_set']=self.new_df['l_sv_gm_lset'],self.new_df['w_sv_gm_lset']

    def compute_games_won(self):
        '''
        Computation of the games won percentage for the winner and loser
        '''
        self.new_df['w_gm_w'],self.new_df['l_gm_w']=self.df['w_games']/(self.df['w_sv_gms']+self.df['l_sv_gms']),self.df['l_games']/(self.df['w_sv_gms']+self.df['l_sv_gms'])
        self.add_percentage_condition('w_gm_w','l_gm_w')

    def compute_game_dominance(self):
        '''
        Computation of game_dominance of the winner and loser
        game_dominance--> return games won/return games won by opponent
        '''
        self.new_df['w_g_dominance'],self.new_df['l_g_dominance']=(self.new_df['w_r_game_w'] / self.new_df['l_r_game_w']).replace((np.nan,np.inf, -np.inf), (0,0,0)),(self.new_df['l_r_game_w'] / self.new_df['w_r_game_w']).replace((np.nan,np.inf, -np.inf), (0,0,0))

    def compute_break_ratio(self):
        '''
        Computation of break ratio of the winner and loser
        break ratio: break points converted(%) / faced break points lost(%)
        '''
        self.new_df['w_br_ratio'],self.new_df['l_br_ratio']=(self.new_df['w_bp_w'] / self.new_df['l_bp_w']).replace((np.nan,np.inf, -np.inf), (0,0,0)),(self.new_df['l_bp_w'] / self.new_df['w_bp_w']).replace((np.nan,np.inf, -np.inf), (0,0,0))

    def compute_winning_percentage(self):
        '''
        Computation winning percentage according to elo of the winner and loser
        '''
        self.new_df['w_win_p']=1/(1+10**((self.df["loser_elo_rating"] - self.df['winner_elo_rating'])/400))
        self.new_df['l_win_p']=1-self.new_df['w_win_p']
        self.add_percentage_condition('w_win_p','l_win_p')
    
    def correct_wrong_rows(self):
        '''
        Correct for wrong rows, in database we found that data got swapped,
        we managed to changed this back to the original data and use the data.
        '''
        df_dict=self.df.to_dict('records')
        for i,row in enumerate(df_dict):
            if df_dict[i]['w_1st_won']/df_dict[i]['w_1st_in']>1 if df_dict[i]['w_1st_in'] != 0 else 0:
                df_dict[i]['w_sv_pt'],df_dict[i]['w_1st_in'],df_dict[i]['w_1st_won'],df_dict[i]['w_2nd_won']=df_dict[i]['w_1st_won']+df_dict[i]['w_2nd_won'],df_dict[i]['w_1st_won'],df_dict[i]['w_1st_in'],df_dict[i]['w_sv_pt']-df_dict[i]['w_1st_in']
                df_dict[i]['l_sv_pt'],df_dict[i]['l_1st_in'],df_dict[i]['l_1st_won'],df_dict[i]['l_2nd_won']=df_dict[i]['l_1st_won'],df_dict[i]['l_1st_in'],df_dict[i]['l_sv_pt']-df_dict[i]['l_1st_in'],df_dict[i]['l_1st_won']+df_dict[i]['l_2nd_won']
        self.df=pd.DataFrame(df_dict)
        return self.df

    def map_columns(self,surface,indoor):
        '''
        Util function for compute_corresonding_rank_elo_rating,
        directly extract the specific elo rating for the specific underground and surface
        
        Parameters
        ----------
        sureface:pd.Series
            The underground of the tennis match
        indoor:pd.Series
            if the match is player outdoor or indoor

        '''
        surface=surface.map({"G":"grass","H":"hard","C":"clay","P":"carpet"})
        indoor=indoor.map({True:'indoor',False:'outdoor'})
        return [surface+"_elo_rating",indoor+"_elo_rating"]
        
    def compute_corresponding_rank_elo_rating(self,df,w_l):
        '''
        Add the rank and elo corresponding to surface and indoor/outdoor to the dataframe
        
        Parameters
        ----------
        df:pd.DataFrame
            The corresponding dataframe with elo rankings
        w_l:String
            String to indicate winner of loser
        '''
        w_l=w_l[0]
        sf_rank, sf_elo, i_o_rank, i_o_elo_rank = ([] for i in range(4))
        df_dict=df.to_dict('records')
        surface_elo,indoor_elo=self.map_columns(df['surface'],df['indoor'])
        for i,row in enumerate(df_dict):
            sf_rank.append(sf_elo.append(row[surface_elo[i]])),i_o_elo_rank.append(row[indoor_elo[i]])
        return pd.DataFrame({'match_id':df['match_id'].copy(),'player_id':df['player_id'].copy(),'date':df['date'].copy(),w_l+'_sf_elo':sf_elo,w_l+'_i_o_elo_rank':i_o_elo_rank})
        
    def compute_rank_elo(self,w_l):
        '''
        Generate the rank/elo for all surfaces and indoor/outdoor for all matches of a specific player
        
        Parameters
        ----------
        extract_data:docker_extract.ExtractData
            Connection to the postgresql database
        w_l:String
            String to indicate winner of loser
        '''
        l_elo_df=self.extract_data.get_elo_rating(self.df[['match_id',w_l+'_id','date','surface','indoor']].rename(columns={w_l+"_id": "player_id"}))
        surface_indoor_rank_elo=self.compute_corresponding_rank_elo_rating(l_elo_df,w_l).drop('date',axis=1)
        self.new_df=surface_indoor_rank_elo.merge(self.new_df,how='right',on='match_id').drop('player_id',axis=1)

    def handle_nan(self):
        self.new_df['w_sf_elo'],self.new_df['w_i_o_elo_rank'] = self.new_df['w_sf_elo'].fillna(self.new_df['w_elo_r']),self.new_df['w_i_o_elo_rank'].fillna(self.new_df['w_elo_r'])
        self.new_df['l_sf_elo'],self.new_df['l_i_o_elo_rank'] = self.new_df['l_sf_elo'].fillna(self.new_df['l_elo_r']),self.new_df['l_i_o_elo_rank'].fillna(self.new_df['l_elo_r'])

    def compute_elo_ranking_surface_indoor(self):
        '''
        Attaches the function to compute rank/elo of all players in a moment of time in the past
        '''
        self.df['date'] =  pd.to_datetime(self.df['date'])
        self.compute_rank_elo('winner')
        self.compute_rank_elo('loser')
        self.handle_nan()

    def clean_odds(self,odds_df):
        odds_df.columns=['date','winner','loser','w_odds','l_odds']
        odds_df['date'] = pd.to_datetime(odds_df['date'])
        odds_df.sort_values(by="date",inplace=True)
        odds_df=odds_df.dropna()
        odds_df['winner']=odds_df['winner'].map(lambda v: ' '.join(v.split(' ')[:-1]))
        odds_df['loser']=odds_df['loser'].map(lambda v: ' '.join(v.split(' ')[:-1]))
        return odds_df


    def get_odds(self):
        page = requests.get("http://www.tennis-data.co.uk/alldata.php")
        odds_df=pd.DataFrame([])
        soup = BeautifulSoup(page.content, 'html.parser')
        links=[link["href"] for link in soup.select('a[href*=".xls"]') if "w" not in link["href"] and int(link['href'][:4])>2001]
        for link in tqdm(links):
            df=pd.read_excel('http://www.tennis-data.co.uk/'+link)[['Date','Winner','Loser','B365W','B365L']]
            odds_df=odds_df.append(df)
        cleaned_df=self.clean_odds(odds_df)
        match_odd_df=self.extract_data.get_match_id(cleaned_df)
        self.new_df=match_odd_df.merge(self.new_df,how='right',on='match_id')

    def clean(self):
        '''
        Applies the condition on the complete dataframe
        '''
        condition_df=pd.DataFrame([])
        for condition in self.new_df_cond:
            condition_df=condition_df.append(condition)
        self.new_df=self.new_df[condition_df.T.all(1)]
        self.new_df.drop(['w_1st_in','l_1st_in','w_sv_gm_lset','l_sv_gm_lset','w_ace_set','l_ace_set','w_df_set','l_df_set','w_r_gm_w_set','l_r_gm_w_set','w_bp_set','l_bp_set','w_bpf_set','l_bpf_set'],axis=1,inplace=True)
        self.new_df[~self.new_df.isin([np.nan, np.inf, -np.inf]).any(1)]
        self.new_df.dropna(inplace=True)
        self.new_df.sort_values(by=['date','match_id'])

    def get_dataframe(self):
        '''
        Returns the dataframe with statistics
        '''
        return self.new_df





