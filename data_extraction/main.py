import os
from utils import generateDataframe
import numpy as np

def main():
    parent_dir=os.path.dirname(os.getcwd())
    if os.path.exists(os.path.join(parent_dir,"TennisML/TennisML_DataFrame.csv")):
        print('exists')
    else:
        generator=generateDataframe(0.23)
        input_dataframe=generator.get_dataframe(to_csv=True)
if __name__=='__main__':
    main()