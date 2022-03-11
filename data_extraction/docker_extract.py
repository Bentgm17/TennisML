from ast import Raise
from unicodedata import name
import docker
from psycopg2 import connect
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from tqdm import tqdm


table_name = "player_v"


class ExtractData():
    """
    A class used to get data from the postgresql dataframe containing the statistics of matches

    ...

    Attributes
    ----------
    conn : psycopg2.connect 
       The connection with the docker file

    Methods
    -------
    gen_ran_data(kwargs)
        Generate and return data on kwargs arguments
    data_on_playerid(player_id,args)
        Generates the property link
    data_on_matchid(player_id,args)
        Generates the property link
    """
    def __init__(self):
        """
        Parameters
        ----------
        url : str
            a formatted string which represents the related site link 
        adress : str
            a formatted string which represents the related adress
        """
        self.conn = create_engine('postgresql://postgres:postgres@localhost:5432/tcb')
    
    def gen_match_data(self,**kwargs):
        """
        Generate and return data on kwargs arguments
        Parameters
        ----------
        **kwargs:
            See below

        :Keyword Arguments:
            * values:list
                Returning values check:https://user-images.githubusercontent.com/2050764/71855801-541ab500-30e2-11ea-8d85-2993b8ebd177.png
                M: Match table
                MS:Match stat table
                i.e-->"M.surface,MS.w_ace"
            * dates:str
                between dates seperated by "->"
                i.e--> "2000-01-01->2020-12-31"

        """
        values=",M.".join([i for i in kwargs["values"]]) if "values" in kwargs else "M.*,MS.*"
        dates="M.date between '{}' and '{}'".format(kwargs["date"].split("->")[0],kwargs["date"].split("->")[1]) if "date" in kwargs else ""
        df = pd.read_sql_query("SELECT {values} from tcb.match M, tcb.match_stats MS WHERE M.match_id=MS.match_id and M.has_stats".format(values=values,dates=dates),con=self.conn)
        df.dropna(how='all', axis=1, inplace=True)
        df=df[df['outcome'] != "RET"]
        df=df[(~df['surface'].isna()) | (~df['indoor'].isna())]
        df.sort_values(by="date",inplace=True)
        return df

    def get_match_details(self,match_id):
        df = pd.read_sql_query("SELECT M.outcome,M.match_id,M.date,P1.first_name,P1.last_name,P2.first_name,P2.last_name from tcb.player P1, tcb.player P2, tcb.match M WHERE M.match_id={match_id} and P1.player_id=M.winner_id and P2.player_id=M.loser_id".format(match_id=match_id),con=self.conn)
        return df

    def get_elo_rating(self,df):
        df.to_sql('player_match_factors', self.conn,if_exists='replace')
        result=pd.read_sql_query("SELECT PMF.match_id,PMF.surface,PMF.indoor,PMF.date,PER.* FROM tcb.player_elo_ranking PER, player_match_factors PMF WHERE PER.rank_date=PMF.date and PER.player_id=PMF.player_id",con=self.conn)  
        return result

    def get_match_id(self,df):
        df.to_sql('player_odds', self.conn,if_exists='replace')
        result=pd.read_sql_query('SELECT M.match_id,po.w_odds,PO.l_odds FROM player_odds PO, tcb.match M, tcb.player P1, tcb.player P2 where M.date=PO.date and P1.player_id=winner_id and P2.player_id=loser_id and P1.last_name=PO.winner and P2.last_name=PO.loser',con=self.conn)  
        return result

    def close_conn(self):
        self.conn.close()
    