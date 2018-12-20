import pandas as pd
import numpy as np
import sys

def preprocessingclaims(servdat,claimdat, servre, preoprefta, admsdat, metri, b, tempsort_d, metricsneedingleftmerge, sizefla):#, reference_optional=None):
    """
    Before implementing this function, the servdat should be preprocessed to restrict it to the metric being examined.
    --- the operative metrics are exempt from this.
    
        Args:
            servdat = services pd.dataframe 
            claimdat = ICD code claims pd.dataframe
            preoprefta = Reference of questionable preop testing pd.dataframe
            admsdat = admission history pd.dataframe
            metri = metric category
            b = Raw ICD code type imported from claims SQL data (depends on whether updating CW criteria or converting claims backward)


    """


    # This code addresses differences between metric structures/definitions
    #################################
    if (metri == 'narc' and admsdat is not None):
        servdat.rename(columns={'EVT_DATE':'TEST_DATE','CODE':'TEST_CODE'},inplace=True)
        servdat.drop(['RX_DOSE' ,'RX_UNIT' ,'DISP_AMT' ,'DISP_UNIT', 'Drug Names','FREQUENCY' ], axis=1, inplace=True)
        servdat=insert_admsdata(servdat,admsdat,metri)
        
        ###############################
        servdat['adms_flag']=False
        servdat.loc[servdat['dys_difftime_noadms_criteria']<tempsort_d,'adms_flag']=True
        servdat['dys_difftime_lastdischarge']=servdat.TEST_DATE.sub(servdat.DIS_DT)/np.timedelta64(1,'D')
        servdat.loc[servdat['dys_difftime_lastdischarge']<tempsort_d,'adms_flag']=True
        print(servdat.columns.values)

        print('size before admission data dropped')
        servdat.drop(['ADM_MRN' ,'ADM_DT', 'DIS_DT','dys_difftime_noadms_criteria'], axis=1, inplace=True)
        print(servdat.columns.values)
        print(sys.getsizeof(servdat))
        
        #to limit memory use
        del admsdat
    elif (metri in ['nonpreop','catpreop'] and admsdat is not None):
        # for card testing in pts w/ surgeries: merge in the surgeries, cull, and then, merge in prior admissions
        servdat_d=servdat[servdat.CODE.isin(servre.code[servre.key==metri])]
        preop_codes=preoprefta.proc_code[preoprefta.key==metri]
        
        #to limit memory use
        del preoprefta
        print('length of servdat_d unique CPT codes:')
        print(len(servdat_d.groupby('CODE').count()))
        servdat=servdat_d.merge(servdat[servdat.CODE.isin(preop_codes)][['MRN','ENC_ID','EVT_DATE','CODE']],on='MRN',how='inner')
        print('length of servdat_d unique CPT codes after restricted to those with surgeries')
        print(len(servdat.groupby('CODE_x').count()))
        #to limit memory use
        del servdat_d
        
        #prep
        servdat.rename(columns={'EVT_DATE_x':'TEST_DATE','CODE_x':'TEST_CODE','CODE_y':'SURG_CODE','ENC_ID_y':'SURG_ENC_ID','EVT_DATE_y':'SURG_DATE'},inplace=True)
        # this line is repetitive for some metrics (lbp, etc)
        servdat=servdat[servdat['TEST_CODE'].isin(servre.code[servre.key==metri])].sort_values('TEST_DATE',ascending=True)
        
        #to limit memory use
        del servre
        
        servdat=servdat[servdat.TEST_DATE<servdat.SURG_DATE] 
        servdat=insert_admsdata(servdat,admsdat,metri)
        
        #to limit memory use
        del admsdat
        ##################################### NEW
        servdat['adms_flag']=False
        servdat.loc[servdat['dys_difftime_noadms_criteria']<tempsort_d,'adms_flag']=True
        servdat['dys_difftime_lastdischarge']=servdat.TEST_DATE.sub(servdat.DIS_DT)/np.timedelta64(1,'D')
        servdat.loc[servdat['dys_difftime_lastdischarge']<tempsort_d,'adms_flag']=True

        
    elif metri in ['lbp','bph','card','cerv','dexa','vitd','feed','psyc']:
        print('renaming servdat columns')
        servdat.rename(columns={'EVT_DATE':'TEST_DATE','CODE':'TEST_CODE'},inplace=True)
    else:
        raise KeyError("Error in preprocessingclaims(), which depends on metric and admsdata being correctly paired.")    

#-------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------
#-- SECOND PHASE OF THIS ALGORITHM    -     START MERGING CLAIMS  --------------------
#-------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------    
    
    #MERGE TO ICD CLAIMS
    print('servdat columns before claims {} merged:'.format(b))
    print(servdat.columns.values)  
    print('sizefla is {}.'.format(sizefla))
    if sizefla==1:
        origservdat=servdat
        
        print('----------------------------------------------')
        servdat=servdat.merge(claimdat,on='MRN',how='left')
        print('size 1')
        origservdat['EVT_DATE']= np.datetime64('2000-01-01')
        origservdat['ICD{}_subcode'.format(b)] = 'ZZZZ_PREPROCESSING_INSERT'
        origservdat['hcclev'] = 'ZZZZ_PREPROCESSING_INSERT'
        origservdat['ccslev'] = 'ZZZZ_PREPROCESSING_INSERT'
        
        print(origservdat.columns.values)
        print('======================')
        print(servdat.columns.values)
        claimdat=None
        servdat.EVT_DATE.fillna(np.datetime64('1900-01-01'), inplace=True)
        servdat = pd.concat([servdat, origservdat])
        print('sizef 2')
        print(servdat.head(1))
    elif metri in metricsneedingleftmerge:
        origservdat=servdat
        origservdat['EVT_DATE']= np.datetime64('2000-01-01')
        servdat=servdat.merge(claimdat,on='MRN',how='left')
        print('check 1')
        origservdat['ICD{}_subcode'.format(b)] = 'ZZZZ_PREPROCESSING_INSERT'
        origservdat['hcclev'] = 'ZZZZ_PREPROCESSING_INSERT'
        origservdat['ccslev'] = 'ZZZZ_PREPROCESSING_INSERT'
        claimdat=None
        servdat.EVT_DATE.fillna(np.datetime64('1900-01-01'), inplace=True)
        print('check 2')
        servdat = pd.concat([servdat, origservdat])
        print('check 3')
    else:
    # for those metrics that didn't require memory restriction
        servdat=servdat.merge(claimdat,on='MRN',how='inner')
        claimdat=None
    servdat=servdat.rename(columns={'EVT_DATE':'CLAIM_DATE'},inplace=False)    
    print('servdat columns after claims merged:')
    print(servdat.columns.values)
    #TIME DIFF BTWN TEST AND CLAIMS
    print('calc dys diff time test')
    servdat['dys_difftime_test_code']=servdat.TEST_DATE.sub(servdat.CLAIM_DATE)/np.timedelta64(1,'D')
    print(sys.getsizeof(servdat))
    print('getting rid of test/service rows merged with claims that came after test/service.')
    servdat=servdat[servdat['dys_difftime_test_code']>=0]
    print(sys.getsizeof(servdat))
    print('sorting claim date')
    servdat=servdat.sort_values(['MRN','CLAIM_DATE'],ascending=True)
    print('drop any w/o ICD')
#    servdat=servdat[~pd.isnull(servdat['ICD{}_subcode'.format(b)])]  
    servdat.dropna(subset=['ICD{}_subcode'.format(b)],inplace=True) 
    print(sys.getsizeof(servdat))
    print('add test date_a')
    servdat['TEST_DATE_a']=servdat['TEST_DATE'].values.astype('datetime64[D]')
    print(sys.getsizeof(servdat))
    return(servdat) 

#######################

def insert_admsdata(servda, admsda, metr):        
#    servda=servda.merge(admsda[['ADM_MRN','ADM_ENC_ID','ADM_DT','DIS_DT']], left_on='MRN', right_on='ADM_MRN', how='left')
    servda=servda.merge(admsda[['ADM_MRN','ADM_DT','DIS_DT']], left_on='MRN', right_on='ADM_MRN', how='left')
    #funnel
    if metr in ['nonpreop','catpreop']:
        servda.rename(columns={'SURG_DATE':'IDX_DATE'},inplace=True)
        servda['IDX_DATE_a']=servda['IDX_DATE'].values.astype('datetime64[D]')
        servda['ADM_DT_a']=servda['ADM_DT'].values.astype('datetime64[D]')
        print('preop loop entered')
        servda.loc[servda['IDX_DATE_a']==servda['ADM_DT_a'],'ADM_DT']=None
        servda.loc[servda['IDX_DATE_a']==servda['ADM_DT_a'],'DIS_DT']=None 
        servda.loc[servda['IDX_DATE_a']==servda['ADM_DT_a'],'ADM_DT_a']=None 
        
    elif metr=='narc':
        servda.rename(columns={'TEST_DATE':'IDX_DATE'},inplace=True)
        servda['IDX_DATE_a']=servda['IDX_DATE'].values.astype('datetime64[D]')
        servda['ADM_DT_a']=servda['ADM_DT'].values.astype('datetime64[D]')
        print('narc loop entered')
    #Need this so the column can be used for .groupby() later
    servda.ADM_DT.fillna(np.datetime64('1900-01-01'), inplace=True)
    servda.ADM_DT_a.fillna(np.datetime64('1900-01-01'), inplace=True)
    servda.DIS_DT.fillna(np.datetime64('1900-01-02'), inplace=True)
    # keep only the rows where the merge matched surgery/prescription that occured after admissions, and select only most recent adms
    servda=servda[servda['IDX_DATE']>=servda['ADM_DT']]
    servda=servda[servda['IDX_DATE']>=servda['DIS_DT']]
    # defunct code: cols=['ADM_MRN','ADM_ENC_ID','ADM_DT', 'DIS_DT','MRN']+[x for x in servda if x not in ['ADM_MRN','ADM_ENC_ID','ADM_DT','DIS_DT','MRN']]
    servda['dys_difftime_noadms_criteria']=servda.IDX_DATE_a.sub(servda.ADM_DT_a)/np.timedelta64(1,'D')
    if metr in ['nonpreop','catpreop']:
        servda.rename(columns={'IDX_DATE':'SURG_DATE'},inplace=True)
    elif metr=='narc':
        servda.rename(columns={'IDX_DATE':'TEST_DATE'},inplace=True)
    else:
        raise ValueError("insert_admsdata function is being misapplied to an invalid metric.")
        
    # code to get only most recent ADM_DT for each index test code & date by patient
    servda_select = servda.groupby(['MRN', 'TEST_CODE', 'TEST_DATE'])['ADM_DT_a'].max()
    servda_select=servda_select.reset_index()
    servda_select['parsed']=1
    servda_select.rename(columns={'MRN':'MRN_s','TEST_CODE':'TEST_CODE_s','TEST_DATE':'TEST_DATE_s',
                                                                'ADM_DT_a':'ADM_DT_a_s'},inplace=True)
    servda=servda.merge(servda_select, 
                         left_on= ['MRN', 'TEST_CODE', 'TEST_DATE','ADM_DT_a'],
                         right_on= ['MRN_s', 'TEST_CODE_s', 'TEST_DATE_s','ADM_DT_a_s'],
                         how= 'inner')
    # code to get only most recent DIS_DT for each index test code, test date and admission date by patient
    servda_select = servda.groupby(['MRN', 'TEST_CODE', 'TEST_DATE','ADM_DT_a'])['DIS_DT'].max()
    servda_select=servda_select.reset_index()
    servda_select['parsed']=1  
    servda_select.rename(columns={'MRN':'MRN_s','TEST_CODE':'TEST_CODE_s','TEST_DATE':'TEST_DATE_s','ADM_DT_a':'ADM_DT_a_s','DIS_DT':'DIS_DT_s'},inplace=True)
    servda=servda.merge(servda_select, 
                         left_on=['MRN', 'TEST_CODE', 'TEST_DATE','ADM_DT_a','DIS_DT'],
                         right_on= ['MRN_s', 'TEST_CODE_s', 'TEST_DATE_s','ADM_DT_a_s','DIS_DT_s'],
                         how= 'inner')           
                         
    servda=servda.drop(['MRN_s_x','TEST_CODE_s_x','TEST_DATE_s_x', 'ADM_DT_a_s_x' ,'parsed_x', 'MRN_s_y',
                        'TEST_CODE_s_y' ,'TEST_DATE_s_y' ,'ADM_DT_a_s_y' ,'DIS_DT_s', 'parsed_y'],axis=1)                                         


    #to limit memory use
    del servda_select
    return (servda)
