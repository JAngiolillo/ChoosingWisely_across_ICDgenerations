import numpy as np
import pandas as pd

def keepindications_removeexclusions(metr,dat,lkupTab,include_or_exclude, lookbac, tempsort_dayx, b, uflagx):
    print('initiating keepIremoveE')
    print(len(dat))
    print('number unique MRNs')
    print(dat.MRN.nunique())    #
    #   sample arguments: 
    #                       metr    = metric
    #                       dat     = servdata
    #                       lkupTab = indications or redflags
    #            include_or_exclude = 'include'
    #                       lookbac = Max restrospective window for valid exclusionary claim
    #                       b       = destination ICD generation 
    #
    if (include_or_exclude=='exclude'):
        ex_dat=dat[(dat.dys_difftime_test_code<=lookbac) & (dat.dys_difftime_test_code>=0)]
        if metr in ['dexa','narc']:
#            ex_dat=ex_dat[ex_dat.dys_difffrom_rCODE<=tempsort_dayx]
            # dump all the rCODEs that are from long ago, but don't dump the rows with relevant ICDs from w/in time window just b/c no relevant CPT
            ex_dat.loc[ex_dat.dys_difffrom_rCODE>tempsort_dayx, 'rCODE']='ERASEDplaceholder' # ERASEDplaceholder will not (likely) wind up in reference table cell, whereas None actually can
        elif metr == 'vitd':
            ex_dat.loc[ex_dat.dys_difffrom_rCODE>365, 'rCODE']='ERASEDplaceholder'
        elif len(lkupTab[lkupTab['class']=='CPT'])>0:
            raise ValueError('the comparison btwn main service code, and prior service codes is invalid. That is to say, prior service codes are not being considered as they should be, given that ref table has a CPT code in it')
        else:
            pass
        print('starting vect_mrn')
        vect_mrn_pd=vect_mrn(metr, ex_dat, lkupTab, b, 'exclude', uflagx)
        ### return only those from the dat.pd which are NOT in the vect_mrn result 
                #(those in time window w/o valid indications, 
                #or any with valid indications that date from beyond lookback period/or result after service performed)
        dat=dat[~dat['MRN'].isin(vect_mrn_pd)]
        return dat
    elif (include_or_exclude=='include'):
        if metr in ['dexa','narc']:
#            in_dat = dat[dat.dys_difffrom_rCODE<=tempsort_dayx]
            in_dat = dat
            in_dat.loc[in_dat.dys_difffrom_rCODE>tempsort_dayx, 'rCODE']='ERASEDplaceholder'
        elif metr == 'vitd':
            in_dat = dat
            in_dat.loc[in_dat.dys_difffrom_rCODE>365, 'rCODE']='ERASEDplaceholder'
        elif len(lkupTab[lkupTab['class']=='CPT'])>0:
            raise ValueError('the comparison btwn main service code, and prior service codes is invalid.')
        else:
            in_dat = dat
        vect_mrn_pd=vect_mrn(metr, in_dat, lkupTab, b, 'include', uflagx)
        dat=dat[dat['MRN'].isin(vect_mrn_pd)]                
        return dat
    else:
        raise ValueError("Set 3rd ARG to 'include' or 'exclude'")


def vect_mrn(met, loc_pd, lkupTa, b, i_or_e, uflagxx):    
    lkupTa_0= lkupTa[lkupTa.startWith==0]
    lkupTa_1= lkupTa[lkupTa.startWith==1]
    if len(lkupTa_1[lkupTa_1['class']!='ICD'])>0:
        raise ValueError('reference table has a startWith stem that has class other than ICD')
    else:
        pass
    print('first row of lkupTab w/o startWith')
    print(lkupTa_0.head(1))
    print('first row of lkupTab with startWith')
    print(lkupTa_1.head(1))
    
    
    if uflagxx == False:    
        vect_mr=np.unique(pd.concat([loc_pd['MRN'][loc_pd.ccslev.astype(str).isin(lkupTa_0['subcode'][lkupTa_0['class']=='CCS'])],
                                     ## following line changed from TEST_CODE to rCODE, for the column of procedures/services merged on queried service
                                     loc_pd['MRN'][loc_pd.rCODE.isin(lkupTa_0['subcode'][lkupTa_0['class']=='CPT'])],
                                     loc_pd['MRN'][loc_pd.hcclev.astype(str).isin(lkupTa_0['subcode'][lkupTa_0['class']=='HCC'])],
                                     loc_pd['MRN'][loc_pd['ICD{}_subcode'.format(b)].isin(lkupTa_0['subcode'][lkupTa_0['class']=='ICD'])]]))
    elif uflagxx == True:
        vect_mr=np.unique(pd.concat([loc_pd['MRN'][loc_pd.rCODE.isin(lkupTa_0['subcode'][lkupTa_0['class']=='CPT'])],
                                     loc_pd['MRN'][loc_pd['ICD{}_subcode'.format(b)].isin(lkupTa_0['subcode'][lkupTa_0['class']=='ICD'])]]))

    else:
        raise KeyError('uflagxx in keepinclusionsremoveexclusions()/vect_mr() is incompatible with algorithm design')
    
    print('starting the startWith chunk')
#    if (np.sum(lkupTa_1['startWith'][lkupTa_1['key']==met])>0): ## This is the original rule for entering startWith chunk
    if len(lkupTa_1)>0:
        print('entered startWith chunk')
        """
        if uflagxx == False:
#            stCodes=lkupTa_1['subcode'][(lkupTa_1['key']==met) & (lkupTa_1['startWith']==1)] ## this is the original selection criteria for stCodes
            stCodes=lkupTa_1['subcode']
            bean=0
            for i in stCodes:      
                print('{} of {} total'.format(bean, len(stCodes)))
                vect_mr=np.append(vect_mr,np.unique(loc_pd['MRN'][loc_pd['ICD{}_subcode'.format(b)].str.startswith(i)]))
                bean+=1

        elif uflagxx==True: 
        """
        lkupTa_1.loc[:,'length']=lkupTa_1['subcode'].apply(lambda x: len(x))
        print('the stems that are length zero: {}'.format(len(lkupTa_1[lkupTa_1.length==0])))
        print(lkupTa_1[lkupTa_1['length']==0])
        lkupTa_1=lkupTa_1[lkupTa_1.length>0]
        lengthsx=lkupTa_1.groupby('length').count()
        lengthsx.reset_index(inplace=True)
        lengthsx=lengthsx['length']

        for n in lengthsx:
            print('stem length is: {}'.format(n))
            t_stCodes=list(lkupTa_1.loc[lkupTa_1.length==n, 'subcode'])
            # create temp dataframe that is light
            t_loc_pd=loc_pd[['MRN','ICD{}_subcode'.format(b)]]
            # create third column
            t_loc_pd.loc[:,'trunc']=t_loc_pd.loc[:,'ICD{}_subcode'.format(b)].str[:n]
            # add those MRNs to the main vect_mr
            vect_mr=np.append(vect_mr,np.unique(t_loc_pd['MRN'][t_loc_pd['trunc'].isin(t_stCodes)]))
            print('keepin/ex stemming cycle completed x1')
#        else:
 #           raise KeyError('startWith Chunk is using incorrect uflagxx')
            #######################################
            ########################################
    print('done with startWith chunk, now parsing for uniques')
    # new line, just for cleanness - restrict to unique MRN
    vect_mr = np.unique(vect_mr)
    print('done with vect_mrn')
    return (vect_mr)
