'''
Created on May 31, 2017

@author: diatr
'''
from _overlapped import NULL
import logging
from xml_parser import utility as ut
import os
from datetime import date, datetime, timedelta
from collections import OrderedDict
from dateutil import parser
import requests
import sys


fields_mapping_file='E:/JohnSnowLabs/Clinical Trials/RSS Data/Update_Fields.csv'
rss_data_path='E:/JohnSnowLabs/Clinical Trials/RSS Data'
rss_updates_file='E:/JohnSnowLabs/Clinical Trials/RSS Data/rss.xml'
update_fields=['title', 'link', 'description', 'guid', 'pubDate']
new_trials_path='E:/JohnSnowLabs/Clinical Trials/RSS Data/NewTrials/'
updates_trials_path='E:/JohnSnowLabs/Clinical Trials/RSS Data/UpdatesTrials/'
rss_url='https://clinicaltrials.gov/show/'

ref_date=datetime.now()-timedelta(days=14)
now=datetime.now()
def get_updated_items(path, filename):
    try:
        os.chdir(path)
        updates_root_node=ut.read_xml(filename)
        return updates_root_node['rss']['channel']['item']
    except:
        print('Something went wrong')
        
        
def download_xml(url, id, path):
    #gets the xml containing information about the clinical trial with nct_id=id and saves it to the provided path
    trial_id={'id':id, 'displayxml':'true'}
    try:
        r = requests.get(url, params=trial_id)
    except:
        logging.error(sys.exc_info()[0])
        print("There were some issues with the file download. For more details check the logs. ")
    try:
        os.chdir(path)
        new_dir=str(date.today())
        os.makedirs(new_dir, exist_ok=True)
        with open(path+new_dir+'/'+id+'.xml', 'w', encoding="utf-8") as f:
            f.write(r.text)
    except:
        logging.error(sys.exc_info()[0])
        print("There were some issues with the local files or folders. For more details check the logs. ")
    
      
def get_new_clinical_trials(items):
    for it in items:
        dt = parser.parse(it['pubDate'])
        trial_id=it['guid']['#text']
        if (dt>ref_date) and (trial_id!=''):
            download_xml(rss_url, trial_id, new_trials_path)
        else:
            download_xml(rss_url, trial_id, updates_trials_path)
            
if __name__ == '__main__':
    #read the updates xml
    logging.basicConfig(filename='data_extractor.log', format='%(asctime)s %(message)s', level=logging.INFO)
    items=get_updated_items(rss_data_path, rss_updates_file)
    get_new_clinical_trials(items)
    
    print("The trials updates were downloaded")