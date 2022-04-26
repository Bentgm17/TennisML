from concurrent.futures import process
from unicodedata import name
import pandas as pd
import numpy as np
from tqdm import tqdm
import sys
from random import getrandbits
import os
import itertools

class computeDataframe:
    """
    Class to get from match based entries to player-point-in-time entries

    ...

    Attributes
    ----------
    df : pd.DataFrame
       Dataframe with the matches as entry
    i:int
        Class counter to reduce computation time
    columns:list
        Get a list of all input variables
    output_df: pd.Dataframe
        Pandas dataframe with the final data

    Methods
    -------
    __init__()
        Initializes the class

    """

    def __init__(self,df,alpha):
        """
        Initializes the class
        Parameters
        ----------
        df:pd.Dataframe
            The dataframe with entries of all matches
        """
        self.alpha=alpha
        self.df_dict=df.to_dict('index')
        self.avg_dict={}
        self.columns=["avg_"+record[2:] for record in df.columns if 'w_' in record if record not in['w_odds','w_sf_elo','w_i_o_elo_rank','w_rank','w_elo_r']]
        self.output_df=pd.DataFrame([])
        self.dictinary_list = []
    
    def update_dict(self,id,row):
        if self.avg_dict.get(id):
            self.avg_dict[id]['count']+=1
            self.avg_dict[id]['stats']=list(map(lambda x,y:self.alpha*x+(1-self.alpha)*y,row,self.avg_dict[id]['stats']))
            # self.avg_dict[id]['stats']=list(map(lambda x,y:x+((y-x)/self.avg_dict[id]['count']),self.avg_dict[id]['stats'],row))
        else:
            self.avg_dict[id]={'stats':row,'count':1,'h2h':{},'tournament':{},'surface':{},'elo':[]}

    def get_h2h(self,winner_id,loser_id):
        records = [value['winner_id'] for _, value in list(self.df_dict.items())[:self.i] if (value['winner_id']==winner_id and value['loser_id']==loser_id) or (value['winner_id']==loser_id and value['loser_id']==winner_id)]
        h2h_perc=(records.count(winner_id))/len(records) if records else 0
        return bool(records),h2h_perc

    def split_player_stats(self,row,w_l):
        opposite='w' if w_l[0]=='l' else 'l'
        stats=[row[stat] for stat in row.keys() if w_l[0]+'_' in stat]
        return row['tournament_id'],row[w_l+'_id'],stats[:5],stats[5:]
    
    def process_elo(self,id,player_elo,k=5):
        prev_elo=self.avg_dict[id]['elo']
        if 0<len(prev_elo)<k:
            prev_elo.append(player_elo)
            return (prev_elo[0]/player_elo)-1
        elif len(prev_elo)==0:
            prev_elo.append(player_elo)
            return 0
        else:
            prev_elo.append(player_elo)
            return(prev_elo.pop(0)/player_elo)-1


    def compute_surface_indoor_strength(self,surface_elo,indoor_elo,rank,player_elo,id):
        elo_dif=self.process_elo(id,player_elo)
        return rank,(surface_elo/player_elo)-1,(indoor_elo/player_elo)-1,elo_dif
    
    def process_player(self,tour_id,id,form_stat,match_stat): 
        try:
            avg_stats=self.avg_dict[id]['stats'].copy()
            self.update_dict(id,match_stat) 
            rank,rel_surf,rel_ind,elo_dif=self.compute_surface_indoor_strength(*form_stat[1:],id)
            p_dict={'rank':rank,'rel_surf':rel_surf,'rel_ind':rel_ind,'elo_dif':elo_dif}
            for i,column in enumerate(self.columns):
                p_dict[column]=avg_stats[i]
            return p_dict
        except Exception as e:
            self.update_dict(id,match_stat) 
            return None

    def combine_dicts(self,p1_dict,p2_dict,label):
        if label:
            return {**{'p_1_'+k: v for k, v in p1_dict.items()}, **{'p_2_'+k: v for k, v in p2_dict.items()}}
        else:
            return {**{'p_1_'+k: v for k, v in p2_dict.items()}, **{'p_2_'+k: v for k, v in p1_dict.items()}}
    
    def process_id_and_h2h(self,winner_id,loser_id,label):
        sl_id,lg_id=sorted((winner_id,loser_id))
        won= (int(winner_id)==int(sl_id))
        try:
            h2h=1
            h2h_percentage=self.avg_dict[sl_id]['h2h'][lg_id]['perc']
            self.avg_dict[sl_id]['h2h'][lg_id]['n']+=1
            self.avg_dict[sl_id]['h2h'][lg_id]['perc']=h2h_percentage+((int(won)-h2h_percentage)/self.avg_dict[sl_id]['h2h'][lg_id]['n'])     
        except Exception as e:
            h2h=0
            h2h_percentage= 1 if not label else 0
            self.avg_dict[sl_id]['h2h'][lg_id]={'n':1.0,'perc':float(won)}
        return (winner_id,loser_id,h2h,h2h_percentage) if label else (loser_id, winner_id,h2h,float(1-h2h_percentage))

    def switch_feature(self,feature_1,feature_2,label):
        if label:
            return feature_1,feature_2
        else:
            return feature_2, feature_1

    def change_h2h(self,h2h,h2h_p,label):
        if h2h==1:
            return h2h_p if label==1 else 1-h2h_p
        return 0

    def compute_tournament_succes(self,tour_id,id,win,column):
        try:
            tour=1
            tour_percentage=self.avg_dict[id][column][tour_id]['perc']
            self.avg_dict[id][column][tour_id]['n']+=1
            self.avg_dict[id][column][tour_id]['perc']=tour_percentage+((int(win)-tour_percentage)/self.avg_dict[id][column][tour_id]['n'])  
        except Exception as e:
            tour=0
            tour_percentage=0
            self.avg_dict[id][column][tour_id]={'n':1.0,'perc':win}   
        return tour,tour_percentage

    
    def process_pre_match_data(self,match_id,row):
        label=bool(getrandbits(1))
        p1_dict=self.process_player(*self.split_player_stats(row,'winner'))
        p2_dict=self.process_player(*self.split_player_stats(row,'loser'))
        (p_1_tournament_succes,p_1_tournament_succes_perc),(p_2_tournament_succes,p_2_tournament_succes_perc)=self.compute_tournament_succes(row['tournament_id'],row['winner_id'],1,'tournament'),self.compute_tournament_succes(row['tournament_id'],row['loser_id'],0,'tournament')
        p1_id,p2_id,h2h,h2h_perc=self.process_id_and_h2h(row['winner_id'],row['loser_id'],label,)
        (p_1_surface,p_1_surface_suc),(p_2_surface,p_2_surface_suc)=self.compute_tournament_succes(row['surface'],row['winner_id'],1,'surface'),self.compute_tournament_succes(row['surface'],row['loser_id'],0,'surface')
        if p1_dict and p2_dict:
            ndict =  self.combine_dicts(p1_dict,p2_dict,label)
            p1_odd,p2_odd=self.switch_feature(row['w_odds'],row['l_odds'],label)
            p_1_tournament_succes,p_2_tournament_succes=self.switch_feature(p_1_tournament_succes,p_2_tournament_succes,label)
            p_1_tournament_succes_perc,p_2_tournament_succes_perc=self.switch_feature(p_1_tournament_succes_perc,p_2_tournament_succes_perc,label)
            p_1_surface_perc,p_2_surface_perc=self.switch_feature(p_1_surface_suc,p_1_surface_suc,label)
            # full_dict={'match_id':match_id,'date':row['date'],'label':int(label),'p1_id':p1_id,'p2_id':p2_id,'p1_odd':p1_odd,'p2_odd':p2_odd,'h2h':h2h,'h2h_p':h2h_perc,'p_1_tour_suc':p_1_tournament_succes,'p_2_tour_suc':p_2_tournament_succes,'p_1_tour_suc_per':p_1_tournament_succes_perc,'p_2_tour_suc_per':p_2_tournament_succes_perc,'p_1_surf_perc':p_1_surface_perc,'p_2_surf_perc':p_2_surface_perc}
            full_dict={'match_id':match_id,'date':row['date'],'label':int(label),'p1_id':p1_id,'p2_id':p2_id,'p1_odd':p1_odd,'p2_odd':p2_odd,'h2h':h2h,'h2h_p':h2h_perc,'p_1_tour_suc':p_1_tournament_succes,'p_2_tour_suc':p_2_tournament_succes,'p_1_tour_suc_per':p_1_tournament_succes_perc,'p_2_tour_suc_per':p_2_tournament_succes_perc}
            full_dict.update(ndict)
            self.dictinary_list.append(full_dict)
    
    def walkover(self):
        for self.i,row in enumerate(tqdm(self.df_dict.items())):
            self.process_pre_match_data(*row)    


    def get_dataframe(self):  
        return pd.DataFrame.from_dict(self.dictinary_list)
