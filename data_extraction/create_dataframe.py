import pandas as pd
import numpy as np
from tqdm import tqdm
import sys
from random import getrandbits
import os

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

    def __init__(self,df):
        """
        Initializes the class
        Parameters
        ----------
        df:pd.Dataframe
            The dataframe with entries of all matches
        """
        self.df_dict=df.to_dict('records')
        self.i=0
        self.columns=["avg_"+record[2:] for record in self.df_dict[0] if 'w_' in record if record not in['w_odds','w_sf_elo','w_i_o_elo_rank','w_rank','w_elo_r']]
        self.output_df=pd.DataFrame([])

    def get_prev_records(self,winner_id,loser_id):
        if self.i>1:
            winner_records=[row for row in self.df_dict[:self.i-1] if row['winner_id']==winner_id or row['loser_id']==winner_id]
            loser_records=[row for row in self.df_dict[:self.i-1] if row['winner_id']==loser_id or row['loser_id']==loser_id]
        else:
            winner_records=[]
            loser_records=[]
        return winner_records,loser_records

    def get_h2h(self,records_p1,loser_id):
        h2h_records=[row for row in records_p1 if row['winner_id']==loser_id or row['loser_id']==loser_id]
        h2h_perc=sum([row['loser_id']==loser_id for row in h2h_records])/len(h2h_records) if h2h_records else 0
        return bool(h2h_records),h2h_perc
    
    def get_match_stats(self,record,won,id):
        return [value for key, value in record.items() if won in key.lower()]
    
    def get_weighted_stats_matches(self,stats):
        return np.mean(np.array(stats[-5:]),axis=0)
    
    def get_rel_fitness(self,elo_rankings):
        return (elo_rankings[0]/elo_rankings[-1])-1

    def transform_to_df(self,stats):
        data_dict={}
        for i in range(len(self.columns)):
            data_dict[self.columns[i]]=stats[i]
        return data_dict

    def get_averages(self,records,id):
        if records:
            w_l={0:'l_',1:'w_'}
            won_records=[record['winner_id']==id for record in records]
            stats=[self.get_match_stats(record,w_l[won_records[i]],id) for i,record in enumerate(records)]
            elo_rankings,stats_matches=[stat[3] for stat in stats[-4:]],[stat[4:] for stat in stats]
            weigthed_stats=self.get_weighted_stats_matches(stats_matches)
            data_dict=self.transform_to_df(weigthed_stats)
            data_dict['rel_fitness']=self.get_rel_fitness(elo_rankings)
            return data_dict
        
    def compute_surface_indoor_strength(self,surface_elo,indoor_elo,player_elo):
        return (surface_elo/player_elo)-1,(indoor_elo/player_elo)-1

    def get_feature_by_label(self,winner_id,loser_id,label):
        if label:
            return winner_id,loser_id
        else:
            return loser_id, winner_id

    def get_prematch_rankings(self,row,w_l):
        rank=row[w_l+'rank']
        surface_strength,indoor_strength=self.compute_surface_indoor_strength(row[w_l+'sf_elo'],row[w_l+'i_o_elo_rank'],row[w_l+'elo_r'])
        return rank,surface_strength,indoor_strength
    
    def combine_dicts(self,p1_dict,p2_dict,label):
        if label:
            return {**{'p_1_'+k: v for k, v in p1_dict.items()}, **{'p_2_'+k: v for k, v in p2_dict.items()}}
        else:
            return {**{'p_1_'+k: v for k, v in p2_dict.items()}, **{'p_2_'+k: v for k, v in p1_dict.items()}}

        
    def process_pre_match_data(self,row):
        label=bool(getrandbits(1))
        prev_rec_p1,prev_rec_p2=self.get_prev_records(row['winner_id'],row['loser_id'])
        p1_dict=self.get_averages(prev_rec_p1,row['winner_id'])
        p2_dict=self.get_averages(prev_rec_p2,row['loser_id'])
        if p1_dict is not None and p2_dict is not None:
            p1_dict['rank'],p1_dict['sf_strength'],p1_dict['in_strength']=self.get_prematch_rankings(row,'w_')
            p2_dict['rank'],p2_dict['sf_strength'],p2_dict['in_strength']=self.get_prematch_rankings(row,'l_')
            h2h,h2h_p=self.get_h2h(prev_rec_p1,row['loser_id'])
            ndict =  self.combine_dicts(p1_dict,p2_dict,label)
            p1_id,p2_id=self.get_feature_by_label(row['winner_id'],row['loser_id'],label)
            p1_odd,p2_odd=self.get_feature_by_label(row['w_odds'],row['l_odds'],label)
            full_dict={'match_id':row['match_id'],'date':row['date'],'label':int(label),'p1_id':p1_id,'p2_id':p2_id,'h2h':h2h,'h2h_p':h2h_p,'p1_odd':p1_odd,'p2_odd':p2_odd}
            full_dict.update(ndict)
            self.output_df=self.output_df.append(full_dict,ignore_index=True)
    
    def walkover(self):
        for self.i,row in enumerate(tqdm(self.df_dict)):
            self.process_pre_match_data(row)

    def get_dataframe(self):
        return self.output_df
            



