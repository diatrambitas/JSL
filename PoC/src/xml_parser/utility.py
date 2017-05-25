'''
Created on May 22, 2017

@author: diatr
'''
import numpy as np
import pandas as pd
from xml.etree import ElementTree
import re

def read_fields_dictionary(file_name):
    #reads the fields mapping csv and builds a dictionary where the keys 
    #are the XML element names and the values are the target CSV column names
    data=pd.read_csv(file_name, header=0, index_col=0, sep=',', na_values=['Nothing'])
    #dic=data.set_index("XML_Element").to_dict()
    dic=data.to_dict()
    return dic["CSV_Column_Name"]

def print_dictionary(dic):
    #iterates over keys in a dictionary and prints key-value pairs
    for k in dic.keys():
        print(k+":"+str(dic[k])+'\n')
        
def clean_line(line):
    if line != "":
        line = line.replace('\n', "")
        line = re.sub('\s+', " ", line)
        line = line.strip()
        line =line.replace(',', ' ')
    return line
   
if __name__ == '__main__':
    pass
fields_mapping_file='E:/JohnSnowLabs/Clinical Trials/FieldsMapping2.csv'
fields_map=read_fields_dictionary(fields_mapping_file)
#print_dictionary(fields_map)