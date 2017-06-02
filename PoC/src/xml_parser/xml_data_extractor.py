'''
Created on May 29, 2017

@author: diatr
'''
from pandas.core.config_init import doc
from collections import OrderedDict
import xml_parser.utility  as ut
from builtins import list, int
from _overlapped import NULL
from datetime import date
import os 
import logging
import datetime
import sys

fields_mapping_file='C:/Users/diatr/git/JSL/PoC/src/xml_parser/FieldsMappingLogicalOrder.csv'
obvious_fields=['textblock', 'country', 'name','last_name','#text', '@type', 'mesh_term', 'address', 'city', 'state', 'zip', 'name_or_title', 'phone', 'email']
sample_data_path='C:/Users/diatr/git/JSL/PoC/src/xml_parser/sample_data'
new_rss_trials='E:/JohnSnowLabs/Clinical Trials/RSS Data/NewTrials/2017-06-01'
results_db_dump='C:/Users/diatr/git/JSL/PoC/src/xml_parser/Results DB dump 3.05.2017'
root_field="clinical_study"
res={} #the dictionary for the CSV file
ignored_elements={}
fields_map={}
current_item=""
batch=20 #the number of rows in the resulting csv files
    
def flatten_structure(d):
    #Gets the flatten and cleaned value for a tree or list structure
    global current_item
    try:
        if type(d) in [str, int, float, date]:
            #simple values are cleaned and returned
            current_item+=ut.clean_line(d)+'|'
        elif type(d)==list:
            #complex values are recursively flattened
            for e in d:
                flatten_structure(e)
        elif type(d) in [OrderedDict, dict]:
            for key in d:
                if key not in obvious_fields:
                        current_item+=key+":"
                flatten_structure(d[key])
    except TypeError as err:
        logging.error(str(err))
        logging.error("the error occured when working on "+str(d)+" "+str(type(d)))
                
def build_csv_dictionary(node_value, node_name=""):
    #Extracts data from the node_value XML node into a dictionary res.
    #Uses the fields_map to generate the appropriate dictionary keys for the extracted data.  
    global fields_map
    global current_item
    global res
    #logging.info("verifying "+node_name+" of type "+str(type(node_value))+"\n")
    if node_name in fields_map:
        #the node value is added to the result set
        if type(node_value) in [str, int, float, date]:
            res[fields_map[node_name]]=ut.clean_line(str(node_value)) if fields_map[node_name] not in res else res[fields_map[node_name]]+"|"+ut.clean_line(str(node_value))
        else:
            #the complex values are flattened 
            current_item=""
            flatten_structure(node_value)
            if fields_map[node_name] in res:
                res[fields_map[node_name]]+=current_item
            else:
                res[fields_map[node_name]]=current_item
      
    elif type(node_value) in [str, int, float, date]:
        #the element is ignored
        ignored_elements[node_name]=node_value
    elif type(node_value) in [OrderedDict, dict]:
        #the node is further explored
            for k in node_value.keys():
                build_csv_dictionary(node_value[k], k)
    elif type(node_value)==list:
        #the list is further explored
            for e in node_value:
                build_csv_dictionary(e, node_name)   
    

def build_csv_from_xml_batch(xml_path, fields_map, csv_file_name, batch_size=5000):
    global res
    all_results=[]
    start_time=datetime.datetime.now()
    current_batch=0;
    print("processing batch "+str(current_batch))
    before=datetime.datetime.now()
    os.chdir(xml_path)
    for f in os.listdir(xml_path):
        if f.endswith(".xml"):
            node=ut.read_xml(xml_path+'/'+f)
            if node!=NULL:
                res={}
                #logging.info("Importing data from "+str(f))
                build_csv_dictionary(node, root_field)
                all_results+=[res]
            index=len(all_results)
            if index > batch:
                ut.export_data(all_results, csv_file_name+str(current_batch)+'.csv', fields_map.values())
                del all_results
                after=datetime.datetime.now()
                logging.info("Processing of batch "+str(current_batch)+" lasted "+str(after-before))
                current_batch+=1
                print("processing batch "+str(current_batch))
                logging.info("Processing batch "+str(current_batch)+"...")
                all_results=[]
                logging.info('Memory used after delete: '+str(sys.getsizeof(all_results)))
                before=datetime.datetime.now()
    #if we still have some data that has been processed it is saved
    if index>0:
        ut.export_data(all_results, csv_file_name+str(current_batch)+'.csv', fields_map.values())
        after=datetime.datetime.now()
        logging.info("Processing of batch "+str(current_batch)+" lasted "+str(after-before))
        del all_results
         
    finish_time=datetime.datetime.now()
    logging.info('Total processing time '+str(finish_time-start_time))
    
    
if __name__ == '__main__':
    
    logging.basicConfig(filename='data_extractor.log', format='%(asctime)s %(message)s', level=logging.INFO)
    fields_map=ut.read_fields_dictionary(fields_mapping_file)
    build_csv_from_xml_batch(sample_data_path, fields_map, sample_data_path+'/CTresults', 20)
# 
#     fields_map=ut.read_fields_dictionary(fields_mapping_file)
#     start_time=datetime.datetime.now()
#     current_batch=0;
#     print("processing batch "+str(current_batch))
#     before=datetime.datetime.now()
#     os.chdir(results_db_dump)
#     for f in os.listdir(results_db_dump):
#         if f.endswith(".xml"):
#             node=ut.read_xml(results_db_dump+'/'+f)
#             if node!=NULL:
#                 res={}
#                 #logging.info("Importing data from "+str(f))
#                 build_csv_dictionary(node, root_field)
#                 all_results+=[res]
#             index=len(all_results)
#             if index >= batch:
#                 ut.export_data(all_results, 'ResultsClinicalTrials'+str(current_batch)+'.csv', fields_map.values())
#                 logging.info('Memory used before delete: '+str(sys.getsizeof(all_results)))
#                 del all_results
#                 after=datetime.datetime.now()
#                 logging.info("Processing of batch "+str(current_batch)+" lasted "+str(after-before))
#                 current_batch+=1
#                 print("processing batch "+str(current_batch))
#                 logging.info("Processing batch "+str(current_batch)+"...")
#                 all_results=[]
#                 logging.info('Memory used after delete: '+str(sys.getsizeof(all_results)))
#                 before=datetime.datetime.now()
#     finish_time=datetime.datetime.now()
#     logging.info('Total processing time '+str(finish_time-start_time))
    print("Done!")
