# echo on
'''
Created on May 22, 2017

@author: diatr
'''
import xmltodict
import pandas as pd
import flatten_json
from pandas.core.config_init import doc
from pandas.io.json import json_normalize
from collections import OrderedDict
import xml_parser.utility  as ut
from builtins import list
import csv
import os
import sys
#from glob import glob
from os import walk
from pandas.core.frame import DataFrame
from _overlapped import NULL
    
file='test.xml'
fields_mapping_file='E:/JohnSnowLabs/Clinical Trials/FieldsMapping2.csv'
sample_data_path='E:/WORK/Eclipse neon/proiecte/PoC/src/xml_parser/sample_data'
root_field="clinical_study"
res={} #the dictionary for the CSV file
all_results=[]
ignored_elements={}
value=""
list_value=""
def read_xml(file_name):
    #reads an xml file and populates the dictionary doc
    if file_name.endswith(".xml"):
        with open(file_name) as fd:
            dic = xmltodict.parse(fd.read())
        return dic
    else:
        return NULL          

def xml_2_flatten_dict(json):
    dic=flatten_json.flatten(json)
def flatten_list(list):
    global list_value
    

def flatten_node_value(node_dic):
    global value
    for k in node_dic.keys():
        if k!="@type":
            if k=="@text" or type(node_dic[k])==str:
                value+=k+":"+ut.clean_line(str(node_dic[k]))+"|"
            elif type(node_dic[k])==OrderedDict:
                value+=flatten_node_value(node_dic[k])
    return value[:-1]

def flatten_list_value(list):
    global value
    for el in list:
        if el!="@type":
            if el=="@text" or type(el)==str:
                value+=el+":"+ut.clean_line(str(el))+"|"
            elif type(el)==OrderedDict:
                value+=flatten_node_value(el)
            elif type(el)==list:
                value+=flatten_list_value(el)
    return value[:-1]
    
def gen_results_dict(node, fields_map):
    #node is the root node (clinical_study) of the xml
    #fields_map is a dictionary of xml node names and their correspondents from the target csv 
    global res
    global ignored_elements
    global value
    root_elements = node if type(node) == OrderedDict else [node] 
    for key in root_elements.keys():
        if key in fields_map:
            #if this is a field in the fields mapping dictionary we have to extract its value
            if type(root_elements[key])==OrderedDict:
                #flatten the value to a string value and add it to the res dictionary
                value=""
                res[fields_map[key]]=ut.clean_line(flatten_node_value(root_elements[key]))
            elif type(root_elements[key])==list:
                value=""
                res[fields_map[key]]=ut.clean_line(flatten_list_value(root_elements[key]))
            else:
                res[fields_map[key]]=res[fields_map[key]]+ut.clean_line(root_elements[key]) if fields_map[key] in res.keys() else ut.clean_line(root_elements[key])
        elif type(root_elements[key])==OrderedDict:
            #the current key field is a dictionary and must be further explored 
            gen_results_dict(root_elements[key], fields_map)
        #if the key is not in the csv fields dictionary and it is a simple field it will be ignored
        else:
            ignored_elements[key]=root_elements[key]

def print_to_csv(dic, file_name):
    with open(file_name, 'w') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, dic.keys())
        w.writeheader()
        w.writerow(dic)
def print_df_to_csv(df, file_name):
    df.to_csv(file_name, header=True)


def export_data(list, file_name, keys):
    with open(file_name, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(list)
    
      
def get_xml_files(path):
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
    return f


if __name__ == '__main__':
    fields_map=ut.read_fields_dictionary(fields_mapping_file)
    results_df=pd.DataFrame()
    os.chdir(sample_data_path)
    for f in os.listdir(sample_data_path):
        node_dict=read_xml(f)
        if (node_dict!=NULL):
            print("Adding data from "+str(f))
            res={}
            try:
                print("Current results before:"+str(sys.getsizeof(res)))
                gen_results_dict(node_dict[root_field], fields_map)
                print("Current results after:"+str(sys.getsizeof(res)))
                 
            except MemoryError as e:
                print("Too much stuff in memory!!!"+str(e))
            all_results+=[res]
            print("Current size of all results "+str(sys.getsizeof(all_results)))
    results_df=pd.DataFrame(all_results)
    export_data(all_results, 'results2.csv', fields_map.values())
    print("Done!")

        
        
        
