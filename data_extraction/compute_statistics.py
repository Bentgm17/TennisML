from re import S
from unicodedata import name
import pandas as pd
from docker_extract import ExtractData
import os
import numpy as np
from tqdm import tqdm

class Transformation():

    def __init__(self,df):
        self.df=df
        self.new_df=pd.DataFrame([])
        self.new_df_cond=[]
    
    def __call__(self):
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
        self.compute_games_won(),self.compute_game_dominance(),self.compute_break_ratio(),self.compute_winning_percentage()

    
    def add_percentage_condition(self,column1,column2):
         self.new_df_cond.append(self.new_df[column1].between(0, 1))
         self.new_df_cond.append(self.new_df[column2].between(0, 1))

    def copy_match_and_playerid(self):
        self.new_df['match_id']=self.df['match_id']
        self.new_df['winner_id']=self.df['winner_id']
        self.new_df['loser_id']=self.df['loser_id']
        self.new_df['date']=pd.to_datetime(self.df['date'])

    def compute_ace(self):
        self.new_df['w_ace'],self.new_df['l_ace']=self.df['w_ace']/self.df['w_sv_pt'],self.df['l_ace']/self.df['l_sv_pt']
        self.add_percentage_condition('w_ace','l_ace')

    def compute_df(self):
        self.new_df['w_df'],self.new_df['l_df']=self.df['w_df']/self.df['w_sv_pt'],self.df['l_df']/self.df['l_sv_pt']
        self.add_percentage_condition('w_df','l_df')

    def compute_total_points(self):
        self.new_df['w_tt_p']=((self.df['w_1st_won']+self.df['w_2nd_won'])+(self.df['l_sv_pt']-(self.df['l_1st_won']+self.df['l_2nd_won'])))/(self.df['l_sv_pt']+self.df['w_sv_pt'])
        self.new_df['l_tt_p']=((self.df['l_1st_won']+self.df['l_2nd_won'])+(self.df['w_sv_pt']-(self.df['w_1st_won']+self.df['w_2nd_won'])))/(self.df['w_sv_pt']+self.df['l_sv_pt'])
        self.add_percentage_condition('w_tt_p','l_tt_p')

    def compute_points_won_on_serve(self):
        self.new_df['w_sv_w'],self.new_df['l_sv_w']=(self.df['w_1st_won']+self.df['w_2nd_won'])/self.df['w_sv_pt'],(self.df['l_1st_won']+self.df['l_2nd_won'])/self.df['l_sv_pt']
        self.add_percentage_condition('w_sv_w','l_sv_w')

    def compute_points_won_on_first_serve(self):
        self.new_df['w_1st_w'],self.new_df['l_1st_w']=self.df['w_1st_won']/self.df['w_1st_in'],self.df['l_1st_won']/self.df['l_1st_in']
        self.add_percentage_condition('w_1st_w','l_1st_w')

    def compute_points_won_on_second_serve(self):
        self.new_df['w_2nd_w'],self.new_df['l_2nd_w']=self.df['w_2nd_won']/(self.df['w_sv_pt']-self.df['w_1st_in']),self.df['l_2nd_won']/(self.df['l_sv_pt']-self.df['l_1st_in'])
        self.add_percentage_condition('w_2nd_w','l_2nd_w')

    def compute_break_points_saved(self):
        self.new_df['w_bp_save'],self.new_df['l_bp_save']=self.df['w_bp_sv']/self.df['w_bp_fc'],self.df['l_bp_sv']/self.df['l_bp_fc']
        self.add_percentage_condition('w_bp_save','l_bp_save')

    def compute_return_points_won_on_return_first_serve(self):
        self.new_df['w_r_1st_w'],self.new_df['l_r_1st_w']=1-self.new_df['l_1st_w'],1-self.new_df['w_1st_w']
        self.add_percentage_condition('w_bp_save','l_bp_save')

    def compute_return_points_won_on_return_second_serve(self):
        self.new_df['w_r_1st_w'],self.new_df['l_r_1st_w']=1-self.new_df['l_1st_w'],1-self.new_df['w_1st_w']
        self.add_percentage_condition('w_r_1st_w','l_r_1st_w')

    def compute_break_points_won(self):
        self.new_df['w_bp_w'],self.new_df['l_bp_w']=1-self.new_df['l_bp_save'],1-self.new_df['w_bp_save']
        self.add_percentage_condition('w_bp_w','l_bp_w')

    def compute_return_points_won(self):
        self.new_df['w_rp_w'],self.new_df['l_rp_w']=1-self.new_df['l_sv_w'],1-self.new_df['w_sv_w']
        self.add_percentage_condition('w_rp_w','l_rp_w')

    def compute_point_dominance(self):
        self.new_df['w_p_dominance'],self.new_df['l_p_dominance']=self.new_df['w_rp_w']/self.new_df['l_rp_w'],self.new_df['l_rp_w']/self.new_df['w_rp_w']

    def compute_numbers_of_first_serve_made(self):
        self.new_df['w_1st_in'],self.new_df['l_1st_in']=self.df['w_1st_in']/self.df['w_sv_pt'],self.df['l_1st_in']/self.df['l_sv_pt']
        self.add_percentage_condition('w_1st_in','l_1st_in')

    def compute_aces_per_game(self):
        self.new_df['w_ace_game'],self.new_df['l_ace_game']=self.df['w_ace']/self.df['w_sv_gms'],self.df['l_ace']/self.df['l_sv_gms']

    def compute_df_per_game(self):
        self.new_df['w_df_game'],self.new_df['l_df_game']=self.df['w_df']/self.df['w_sv_gms'],self.df['l_df']/self.df['l_sv_gms']

    def compute_aces_per_set(self):
        self.new_df['w_ace_set'],self.new_df['l_ace_set']=self.df['w_ace']/(self.df['w_sets']+self.df['l_sets']),self.df['l_ace']/(self.df['w_sets']+self.df['l_sets'])

    def compute_df_per_set(self):
        self.new_df['w_df_set'],self.new_df['l_df_set']=self.df['w_df']/(self.df['w_sets']+self.df['l_sets']),self.df['l_df']/(self.df['w_sets']+self.df['l_sets'])

    def compute_df_per_second_serve(self):
        self.new_df['w_df_2nd_sv'],self.new_df['l_df_2nd_sv']=self.df['w_df']/(self.df['w_sv_pt']-self.df['w_1st_in']),self.df['l_df']/(self.df['l_sv_pt']-self.df['l_1st_in'])
        self.add_percentage_condition('w_df_2nd_sv','l_df_2nd_sv')
        
    def compute_aces_per_df(self):
        self.new_df['w_ace_df_ratio'],self.new_df['l_ace_df_ratio']=(self.new_df['w_ace'] / self.new_df['w_df']).replace((np.nan,np.inf, -np.inf), (0,0,0)),(self.new_df['l_ace'] / self.new_df['l_df']).replace((np.nan,np.inf, -np.inf), (0,0,0))

    def compute_first_serve_efficiency(self):
        self.new_df['w_1st_sv_eff'],self.new_df['l_1st_sv_eff']=self.new_df['w_1st_w']/self.new_df['w_2nd_w'],self.new_df['l_1st_w']/self.new_df['l_2nd_w']

    def compute_serve_ip_efficiency(self):
        self.new_df['w_sv_ip_w'],self.new_df['l_sv_ip_w']=(self.df['w_1st_won']+self.df['w_2nd_won']-self.df['w_ace']-self.df['w_df'])/(self.df['w_sv_pt']-self.df['w_ace']-self.df['w_df']),(self.df['l_1st_won']+self.df['l_2nd_won']-self.df['l_ace']-self.df['l_df'])/(self.df['l_sv_pt']-self.df['l_ace']-self.df['l_df'])
        self.add_percentage_condition('w_sv_ip_w','l_sv_ip_w')

    def compute_points_lost_per_serv_game(self):
        self.new_df['w_ploss_game'],self.new_df['l_ploss_game']=(self.df['w_sv_pt']-self.df['w_1st_won']-self.df['w_2nd_won'])/self.df['w_sv_gms'],(self.df['l_sv_pt']-self.df['l_1st_won']-self.df['l_2nd_won'])/self.df['l_sv_gms']

    def compute_break_points_faced_per_game(self):
        self.new_df['w_bpf_game'],self.new_df['l_bpf_game']=self.df['w_bp_fc']/self.df['w_sv_gms'],self.df['l_bp_fc']/self.df['l_sv_gms']

    def compute_break_points_faced_per_set(self):
        self.new_df['w_bpf_set'],self.new_df['l_bpf_set']=self.df['w_bp_fc']/(self.df['w_sets']+self.df['l_sets']),self.df['l_bp_fc']/(self.df['w_sets']+self.df['l_sets'])

    def compute_service_games_won(self):
        self.new_df['w_svp_game'],self.new_df['l_svp_game']=1-(self.df['w_bp_fc']-self.df['w_bp_sv'])/self.df['w_sv_gms'],1-(self.df['l_bp_fc']-self.df['l_bp_sv'])/self.df['l_sv_gms']
        self.add_percentage_condition('w_svp_game','l_svp_game')

    def compute_sv_games_lost_per_set(self):
        self.new_df['w_sv_gm_lset'],self.new_df['l_sv_gm_lset']=(self.df['w_bp_fc']-self.df['w_bp_sv'])/(self.df['w_sets']+self.df['l_sets']),(self.df['l_bp_fc']-self.df['l_bp_sv'])/(self.df['w_sets']+self.df['l_sets'])

    def compute_ip_return_points(self):
        self.new_df['w_r_ip_w'],self.new_df['l_r_ip_w']=1-self.new_df['l_sv_ip_w'],1-self.new_df['w_sv_ip_w']
        self.add_percentage_condition('w_r_ip_w','l_r_ip_w')

    def compute_points_won_per_return_game(self):
        self.new_df['w_pwon_r_game'],self.new_df['l_pwon_r_game']=self.new_df['l_ploss_game'],self.new_df['w_ploss_game']

    def compute_bp_per_game(self):
        self.new_df['w_bp_game'],self.new_df['l_bp_game']=self.new_df['l_bpf_game'],self.new_df['w_bpf_game']

    def compute_bp_per_set(self):
        self.new_df['w_bp_set'],self.new_df['l_bp_set']=self.new_df['l_bpf_set'],self.new_df['w_bpf_set']

    def compute_return_games_won(self):
        self.new_df['w_r_game_w'],self.new_df['l_r_game_w']=1-self.new_df['l_svp_game'],1-self.new_df['w_svp_game']
        self.add_percentage_condition('w_r_game_w','l_r_game_w')

    def compute_return_games_won_per_set(self):
        self.new_df['w_r_gm_w_set'],self.new_df['l_r_gm_w_set']=self.new_df['l_sv_gm_lset'],self.new_df['w_sv_gm_lset']

    def compute_games_won(self):
        self.new_df['w_gm_w'],self.new_df['l_gm_w']=self.df['w_games']/(self.df['w_sv_gms']+self.df['l_sv_gms']),self.df['l_games']/(self.df['w_sv_gms']+self.df['l_sv_gms'])
        self.add_percentage_condition('w_gm_w','l_gm_w')

    def compute_game_dominance(self):
        self.new_df['w_g_dominance'],self.new_df['l_g_dominance']=(self.new_df['w_r_game_w'] / self.new_df['l_r_game_w']).replace((np.nan,np.inf, -np.inf), (0,0,0)),(self.new_df['l_r_game_w'] / self.new_df['w_r_game_w']).replace((np.nan,np.inf, -np.inf), (0,0,0))

    def compute_break_ratio(self):
        self.new_df['w_br_ratio'],self.new_df['l_br_ratio']=(self.new_df['w_bp_w'] / self.new_df['l_bp_w']).replace((np.nan,np.inf, -np.inf), (0,0,0)),(self.new_df['l_bp_w'] / self.new_df['w_bp_w']).replace((np.nan,np.inf, -np.inf), (0,0,0))

    def compute_winning_percentage(self):
        self.new_df['w_win_p']=1/(1+10**((self.df["loser_elo_rating"] - self.df['winner_elo_rating'])/400))
        self.new_df['l_win_p']=1-self.new_df['w_win_p']
        self.add_percentage_condition('w_win_p','l_win_p')
    
    def correct_wrong_rows(self):
        df_dict=self.df.to_dict('records')
        for i,row in enumerate(df_dict):
            if df_dict[i]['w_1st_won']/df_dict[i]['w_1st_in']>1:
                df_dict[i]['w_sv_pt'],df_dict[i]['w_1st_in'],df_dict[i]['w_1st_won'],df_dict[i]['w_2nd_won']=df_dict[i]['w_1st_won']+df_dict[i]['w_2nd_won'],df_dict[i]['w_1st_won'],df_dict[i]['w_1st_in'],df_dict[i]['w_sv_pt']-df_dict[i]['w_1st_in']
                df_dict[i]['l_sv_pt'],df_dict[i]['l_1st_in'],df_dict[i]['l_1st_won'],df_dict[i]['l_2nd_won']=df_dict[i]['l_1st_won'],df_dict[i]['l_1st_in'],df_dict[i]['l_sv_pt']-df_dict[i]['l_1st_in'],df_dict[i]['l_1st_won']+df_dict[i]['l_2nd_won']
        self.df=pd.DataFrame(df_dict)
        return self.df

    def map_columns(self,surface,indoor):
        surface=surface.map({"G":"grass","H":"hard","C":"clay","P":"carpet"})
        indoor=indoor.map({True:'indoor',False:'outdoor'})
        return [surface+"_rank", surface+"_elo_rating",indoor+"_rank",indoor+"_elo_rating"]
        
    def compute_corresponding_rank_elo_rating(self,df,w_l):
        w_l=w_l[0]
        sf_rank, sf_elo, i_o_rank, i_o_elo_rank = ([] for i in range(4))
        df_dict=df.to_dict('records')
        surface_rank,surface_elo,indoor_rank,indoor_elo=self.map_columns(df['surface'],df['indoor'])
        for i,row in enumerate(df_dict):
            sf_rank.append(row[surface_rank[i]]),sf_elo.append(row[surface_elo[i]]),i_o_rank.append(row[indoor_rank[i]]),i_o_elo_rank.append(row[indoor_elo[i]])
        return pd.DataFrame({'match_id':df['match_id'].copy(),'player_id':df['player_id'].copy(),'date':df['date'].copy(),w_l[0]+'_sf_rank':sf_rank,w_l+'_sf_elo':sf_elo,w_l+'_i_o_rank':i_o_rank,w_l+'_i_o_elo_rank':i_o_elo_rank})
        
    def compute_rank_elo(self,extract_data,w_l):
        l_elo_df=extract_data.get_elo_rating(self.df[['match_id',w_l+'_id','date','surface','indoor']].rename(columns={w_l+"_id": "player_id"}))
        surface_indoor_rank_elo=self.compute_corresponding_rank_elo_rating(l_elo_df,w_l).drop('date',axis=1)
        self.new_df=surface_indoor_rank_elo.merge(self.new_df,how='inner',on='match_id').drop('player_id',axis=1)

    def compute_elo_ranking_surface_indoor(self):
        extract_data=ExtractData()
        self.df['date'] =  pd.to_datetime(self.df['date'])
        self.compute_rank_elo(extract_data,'winner')
        self.compute_rank_elo(extract_data,'loser')

    def clean(self):
        condition_df=pd.DataFrame([])
        for condition in self.new_df_cond:
            condition_df=condition_df.append(condition)
        self.new_df=self.new_df[condition_df.T.all(1)]

    def get_dataframe(self):
        self.clean()
        return self.new_df





