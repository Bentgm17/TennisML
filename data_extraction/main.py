import os
from utils import generateDataframe

def main():
    parent_dir=os.path.dirname(os.getcwd())
    if os.path.exists(os.path.join(parent_dir,"TennisML/TennisMLDataFrame.csv")):
        print('exists')
    else:
        generator=generateDataframe()
        input_dataframe=generator.get_dataframe(to_csv=True)

if __name__=='__main__':
    main()