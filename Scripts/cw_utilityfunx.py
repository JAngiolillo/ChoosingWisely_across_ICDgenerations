#import simple utility functions (add_age, add_month, timeBtwnDex, 
#                                 calculate_age, downloadDemographicsByDate, 
#                                 subsetByDemographics, )

import pyodbc, os, math
import pandas as pd
import numpy as np    
import datetime as dt
from datetime import datetime, timedelta
#os.chdir('/home/angiolj/topdir/Jack/')
from DB import DB


def add_age(tmp,demos):
    # As of July 8, 
    # this is used only by Dexa - this is because SQL table J_CW_DEXA omits Gender and DOB, but has MRN
    # demos will be supplied by downloaddemos... function, which has gets gender and DOB for ?all MRNs w/ an encounter in whole DB
    # the tmp will be a pandas dataframe. hence the use of ".merge"
    #
    # speaking of pandas, currently (7/8/16) using pandas 0.16.2 from anaconda - which is not the latest
    #
    #
    # 
    tmp=tmp.merge(demos[['MRN','GENDER','DOB']],on='MRN',how='left')
    tmp.DOB=tmp.DOB.apply(lambda x:pd.to_datetime(x))
    tmp.EVT_DATE=tmp.EVT_DATE.apply(lambda x:pd.to_datetime(x))
    tmp['AgeAtTest']=(tmp.EVT_DATE-tmp.DOB)/np.timedelta64(1,'Y')
    return tmp

def add_month(tempdf,datCol):
    tempdf[datCol+'_month']=pd.to_datetime(tempdf[datCol]).apply(lambda x:str(x.year)+'_'+str(x.month))
    return tempdf

    
def timeBtwnDex(vis,incol):
    vis=vis.sort_values(incol,ascending=True)
    vis['to_next']=vis[incol].shift(-1)-vis[incol]
    # what does this next line do? is it a logical expression saying if x<timedelta, set equal to np.nan?
    vis['to_next'][pd.to_timedelta(vis['to_next'])<timedelta(days=0)]=np.nan
    vis['since_last']=vis[incol]-vis[incol].shift(1)
    vis['since_last'][pd.to_timedelta(vis['since_last'])<timedelta(days=0)]=np.nan
    return(vis)

########################
#Functions copied from masterFunctions.py

def calculate_age(born):
    today = dt.date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def downloadDemographicsByDate(startDt, stopDt):
    #db=DB()
    # selecting *all* from V_PATIENT_MASTER - includes >>10 columns
    #      among them: GENDER, DOB, RACE, ETHNICITY, POSTCODE
    # returns a list of all patients seen in the time window
    #
    # unclear what the use of 'current_age' is.
    sqlStr="SELECT * FROM V_PATIENT_MASTER WHERE MRN IN (SELECT DISTINCT(MRN) FROM V_ENC_MP WHERE ADM_DT BETWEEN '"+startDt+"' AND '"+stopDt+"') OR MRN IN (SELECT DISTINCT(MRN) FROM V_ENC_FS WHERE CONTACT_DT BETWEEN '"+startDt+"' AND '"+stopDt+"');"
    tmp=pd.read_sql(sqlStr,db)
    tmp['DOB']=pd.to_datetime(tmp['DOB'],infer_datetime_format=True,errors='ignore')
    ## ???? is this the current age when the code is run?
    tmp['current_age']=tmp['DOB'].apply(calculate_age)
    return(tmp)

##########################

def df_claimtrimmer(main_df, tgt_codebasis, fullstem_refcodes_list,stemreference_codes_list):
    print('length of main_df entering claimtrimmer')
    print(len(main_df))
    print('progress 1')
    main_df['keep']=0
    main_df.loc[main_df['ICD{}_subcode'.format(tgt_codebasis)].isin(fullstem_refcodes_list),'keep']=1        
    main_df.loc[main_df['hcclev'].isin(fullstem_refcodes_list), 'keep'] = 1
    main_df.loc[main_df['ccslev'].isin(fullstem_refcodes_list), 'keep'] = 1
    print('progress 2')
    try:    
        ref_shortest= min([len(x) for x in stemreference_codes_list])
        ref_longest = max([len(x) for x in stemreference_codes_list])
    except:
        print('ref_shortest and ref_longest determination raised an ERROR')
        main_df = main_df[main_df.keep==1]
        print('length of main_df after claimtrimmer:')
        print(len(main_df))
        return main_df
    if ref_longest == 0: # I don't think this chunk should ever be used given the except clause above
        print("df_trimmer() stemming-step passed because no reference codes present in input reference list")
        main_df = main_df[main_df.keep==1]
        print('length of main_df after claimtrimmer:')
        print(len(main_df))
        return main_df
    else:
        for x in range(ref_shortest,ref_longest+1):
            print('claimtrimmer: #{}'.format(x))
            main_df.loc[main_df['ICD{}_subcode'.format(tgt_codebasis)].str[0:x].isin(stemreference_codes_list),'keep']=1
        main_df = main_df[main_df.keep==1]
        print('length of main_df after claimtrimmer:')
        print(len(main_df))
        return main_df
##########################

def subsetByDemographics(dat,minimumage,maximumage,gendercrit):
    #SUBSET BY DEMOGRAPHICS
    dat=dat[(dat.AgeAtTest>=minimumage) & (dat.AgeAtTest<maximumage)]
    if "All" not in gendercrit:
        dat=dat[dat['GENDER'].isin(gendercrit)]
    return(dat)
##################################################

def deciparsing(stringx):
    stringx
    bean_count = 0
    logic = False
    for x in stringx:
        if x is '.':
            return bean_count
        else:
            bean_count +=1
    if logic is False:
        return 0
        
def prDF(dataframe, x=None, y=4, z=None):
   
    print("***************************")
    print(type(dataframe))
    bean_counter=0
    if x==None:
        print("Column names:")
        for abc in list(dataframe.columns.values):
            print("{}  {!r}".format(bean_counter, abc))
            bean_counter +=1
        print(dataframe.loc[0:y,:z])
    else:
        print("referenced key/dataframe['{}']".format(x))
        print("Column names:")
        for abc in list(dataframe[x].columns.values):
            print("{}  {!r}".format(bean_counter, abc))
            bean_counter +=1
        print(dataframe[x].loc[0:y,:z])
    print("***************************")



