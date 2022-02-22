from ast import Raise
from unicodedata import name
import docker
from psycopg2 import connect
import pandas as pd

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
        self.conn = connect(
        dbname = "tcb",
        user="postgres",
        host="localhost",
        port=5432,
        password="postgres"   
    )

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
        dates="date between '{}' and '{}'".format(kwargs["date"].split("->")[0],kwargs["date"].split("->")[1]) if "date" in kwargs else ""
        df = pd.read_sql_query("SELECT {} from tcb.match M,tcb.match_stats MS WHERE M.match_id=MS.match_id and M.best_of=3 and {}".format(values,dates),con=self.conn)
        return df

    def get_match_details(self,match_id):
        df = pd.read_sql_query("SELECT M.outcome,M.match_id,M.date,P1.first_name,P1.last_name,P2.first_name,P2.last_name from tcb.player P1, tcb.player P2, tcb.match M WHERE M.match_id={match_id} and P1.player_id=M.winner_id and P2.player_id=M.loser_id".format(match_id=match_id),con=self.conn)
        return df
        
    def close_conn(self):
        self.conn.close()

if __name__ == "__main__":
    class_data=ExtractData()
    # print(class_data.get_match_details(80931))

    kwargs={'date':'2000-01-01->2020-12-31'}
    df=class_data.gen_match_data(**kwargs)
    df.to_csv('test_dataframe1.csv')