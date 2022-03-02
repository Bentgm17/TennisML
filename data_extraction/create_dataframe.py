import pandas as pd
import numpy as np
from tqdm import tqdm


class generateDataFrame():

    def __init__(self,groupdf,df):
        self.df=df
        self.df['h2h']=np.nan
        self.groupdf=groupdf

    def add_head_to_head(self):
        for name, group in tqdm(self.groupdf):
            for p2 in group['p2_id'].unique():
                matches_list=group[group['p2_id']==p2]['match_id'].to_list()
        return self.df

    def get_h2h_results(self,p1_num,p2_num):
        player_df=self.get_group(p1_num)
        return player_df[player_df['p2_id']==p2_num]

    def get_group(self,group_num):
        return self.groupdf.get_group(group_num)

class computeDataframe():

    def __init__(self,df):
        self.df=df
        

    def get_player_dataframe(self,w_l):
        stats_cols=[column for column in self.df.columns if w_l[0]+"_" in column]
        p2_w_l='loser' if w_l=='winner' else 'winner'
        columns=['match_id',w_l+'_id',p2_w_l+'_id','date']+stats_cols
        new_cols=['match_id','p1_id','p2_id','date']+[column[2:] for column in stats_cols]
        new_df=self.df[columns]
        new_df.columns=new_cols
        new_df['label']=1 if w_l=='winner' else 0
        return new_df

    def compute_players_dataframe(self):
        winner_df=self.get_player_dataframe('winner')
        loser_df=self.get_player_dataframe('loser')
        players_df=pd.concat([winner_df,loser_df]).sort_values(by='p1_id').reset_index(drop=True)
        groupby_df=players_df.sort_values('date').groupby('p1_id')
        return groupby_df


