import os
import pandas as pd
from create_dataframe import computeDataframe
from docker_extract import ExtractData
from compute_statistics import Transformation
import warnings

warnings.filterwarnings("ignore")

class generateDataframe:

    def dataframe_output(self,df):
        print('Extracting Dataframe......')
        dataframe_computer=computeDataframe(df)
        dataframe_computer.walkover()
        final_df=dataframe_computer.get_dataframe()
        return final_df

    def statistics_add(self,matches):
        print('Initializing......')
        data_trans = Transformation(matches)
        data_trans()
        new_df=data_trans.get_dataframe()
        return self.dataframe_output(new_df)

    def postgres_loader(self):
        print('Starting......')
        postgres_loader=ExtractData()
        kwargs={'date':'1950-01-01->2020-12-31'}
        matches=postgres_loader.gen_match_data(**kwargs)
        return self.statistics_add(matches)

    def main(self):
        return self.postgres_loader()

    def get_dataframe(self,to_csv=False):
        if to_csv:
            dataframe=self.main()
            dataframe.to_csv('TennisML_Dataframe.csv')
            return dataframe
        return self.main()
