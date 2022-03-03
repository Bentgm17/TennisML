import pandas as pd
import numpy as np
from tqdm import tqdm
import sys
from random import getrandbits

class computeDataframe:

    def __init__(self,df):
        self.df_dict=df.to_dict('records')
        self.i=0
        self.columns=["avg_"+record[2:] for record in self.df_dict[0] if 'w_' in record][4:]
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
        return np.mean(np.array(stats),axis=0)
    
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
            rel_fitness=self.get_rel_fitness(elo_rankings)
            if len(records)==8:
                print(data_dict)

                sys.exit()
                print(records[0].keys())
                print(weigthed_ranking)
                sys.exit()
        
        
        # stats_cols=[column for column in self.df.dict[0].keys()]
        
    

    def process_pre_match_data(self,row):
        label=bool(getrandbits(1))
        prev_rec_p1,prev_rec_p2=self.get_prev_records(row['winner_id'],row['loser_id'])
        averages_p1=self.get_averages(prev_rec_p1,row['winner_id'])
        averages_p2=self.get_averages(prev_rec_p2,row['loser_id'])
        rank_winner,rank_loser=row['w_rank'],row['l_rank']
        h2h,h2h_p=self.get_h2h(prev_rec_p1,row['loser_id'])
        return h2h,h2h_p
    
    def walkover(self):
        for self.i,row in tqdm(enumerate(self.df_dict)):
            result,result_p=self.process_pre_match_data(row)
            





# class generateDataFrame():

#     def __init__(self,groupdf,df):
#         self.df=df
#         self.df['h2h']=np.nan
#         self.groupdf=groupdf

#     def add_head_to_head(self):
#         for name, group in tqdm(self.groupdf):
#             for p2 in group['p2_id'].unique():
#                 matches_list=group[group['p2_id']==p2]['match_id'].to_list()
#         return self.df

#     def get_h2h_results(self,p1_num,p2_num):
#         player_df=self.get_group(p1_num)
#         return player_df[player_df['p2_id']==p2_num]

#     def get_group(self,group_num):
#         return self.groupdf.get_group(group_num)

# class computeDataframe():

#     def __init__(self,df):
#         self.df=df
        

#     def get_player_dataframe(self,w_l):
#         stats_cols=[column for column in self.df.columns if w_l[0]+"_" in column]
#         p2_w_l='loser' if w_l=='winner' else 'winner'
#         columns=['match_id',w_l+'_id',p2_w_l+'_id','date']+stats_cols
#         new_cols=['match_id','p1_id','p2_id','date']+[column[2:] for column in stats_cols]
#         new_df=self.df[columns]
#         new_df.columns=new_cols
#         new_df['label']=1 if w_l=='winner' else 0
#         return new_df

#     def compute_players_dataframe(self):
#         winner_df=self.get_player_dataframe('winner')
#         loser_df=self.get_player_dataframe('loser')
#         players_df=pd.concat([winner_df,loser_df]).sort_values(by='p1_id').reset_index(drop=True)
#         groupby_df=players_df.sort_values('date').groupby('p1_id')
#         return groupby_df


