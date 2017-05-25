'''
Created on May 23, 2017

@author: diatr
'''

import pandas as pd
from pandas.io.json import json_normalize
import xml_parser.simple_parser as sp
import xml_parser.utility  as ut
import os
from _overlapped import NULL

fields_mapping_file='E:/JohnSnowLabs/Clinical Trials/FieldsMapping2.csv'
sample_data_path='E:/WORK/Eclipse neon/proiecte/PoC/src/xml_parser/sample_data_20'
def read_df(filename):
    if filename.endswith(".xml"):
        node_dict=sp.read_xml(filename)
        df=json_normalize(node_dict)
        return df
    else:
        return NULL

def save_df_to_file(df, file_path):
    df.to_csv(file_path,sep=",", header=True )

def add_line_to_results(df, fields_map ):
    s=pd.Series([""],index=fields_map.values())
    for c in df.columns:
        found=False
        for k in fields_map.keys():
            if not found and c.find(k)!=-1:
                s[fields_map[k]]=ut.clean_line(str(df[c].values[0]))+"|"
                found=True
    return s

if __name__ == '__main__':
    
    fields_map=ut.read_fields_dictionary(fields_mapping_file)
    data=pd.DataFrame(columns=fields_map.values())
    os.chdir(sample_data_path)
    for f in os.listdir(sample_data_path):
        if f.endswith(".xml"):
            df_xml=read_df(f)
            print(str(f))
            res=add_line_to_results(df_xml, fields_map)
            data=data.append(res, ignore_index=True)
    save_df_to_file(data, "results_df.csv")
    