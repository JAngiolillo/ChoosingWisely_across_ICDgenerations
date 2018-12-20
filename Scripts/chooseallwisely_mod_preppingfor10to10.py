
"""
#           ChooseAllWisely: Unified python function to evaluate Choosing Wisely Metrics with pandas dataframes.
#           ****************************************************************************************************
#
#           Developed by: Colin Walsh, Trent Rosenbloom, John Angiolillo  
#           Vanderbilt University Medical Center - DBMI                
#
"""
from keepindications_removeexclusions_preppingfor10to10 import keepindications_removeexclusions
#from keepindications_removeexclusions import keepindications_removeexclusions
#from preprocessingclaims import preprocessingclaims
from preprocessingclaims_preppingfor10to10 import preprocessingclaims
from cw_utilityfunx import add_month, timeBtwnDex, subsetByDemographics, df_claimtrimmer 
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

def chooseallwisely(metric,                         #0
                    servdata,                       #1
                    claimdata,                      #2
                    demodata,                       #3
                    demoref,                        #4
                    indicref,                       #5
                    redflagref,                     #6
                    servref,                        #7
                    dexadata=None,                  #8
                    preopreftab=None,               #9
                    admsdata=None,                  #10
                    tempsort_day=True,              #11
                    lookback=365,                   #12
                    rootICD_gx=10,                  #13
                    sourcegemdict=None,             #14
                    procgem_dict=None,              #15
                    uniqueflagx=None,               #16
                    second_data=None,                #17
                    window_date = None              #18
                    ):

    #################################################################################################
    output={}
    output_denominator=None
    output_numerator=None
    sizeflag = 0
    
    if uniqueflagx==False:
        a=10 if rootICD_gx==9 else 9
    elif (uniqueflagx==True and rootICD_gx==10):
        a=10 
    else:
        raise KeyError("The dictionary flags are inconsistent \n (review rootICD_gx, sourcegemdict, and uniqueflagx) \n [Working <uniqueflagx> is ={}]".format(uniqueflagx))
    
    if len(servref[servref.startWith==1])>0:
        raise KeyError("servref dataframe codes include stemmed codes (ie those with startWith==1). \nThis is problematic b/c of 'services['CODE'].isin(services.code)' doesn't account for stemmed codes")
    elif len(indicref[indicref.startWith==1].groupby('class').count())>1:
        raise KeyError("indicref dataframe codes include non-ICD stemmed codes (ie those with startWith==1). \n This is a problem b/c df.isin() doesn't account for this")
    elif len(redflagref[redflagref.startWith==1].groupby('class').count())>1:
        raise KeyError("redflagref dataframe codes include non-ICD stemmed codes (ie those with startWith==1). \n This is a problem b/c df.isin() doesn't account for this")
    else:
        print("restrictions on reference startWith code-stems verified.")

    metrics_needing_drugs_as_primary_code_but_still_use_cpts = ['narc','psyc']
    print('FYI the metrics included in drugs with cpts: {}'.format(metrics_needing_drugs_as_primary_code_but_still_use_cpts))
    if metric in metrics_needing_drugs_as_primary_code_but_still_use_cpts:
        if second_data is None:
            raise KeyError('The second_data parameter is None, which is incompatible with algorithm.')
        else:
            print('second_data.head(3) is:')
            second_data=second_data[['MRN','CODE','EVT_DATE']]
            second_data.rename(columns={'MRN':'rMRN','CODE':'rCODE','EVT_DATE':'rEVT_DATE'}, inplace=True)
            print(second_data.head(3))       
    else:
        pass
    #Subset by Demographics
    minage=demoref['agemin'].loc[demoref['key']==metric].tolist()
    maxage=demoref['agemax'].loc[demoref['key']==metric].tolist()
    gend=demoref['gendercrit'].loc[demoref['key']==metric].tolist()
    print('minage: {}'.format(minage))
    print('maxage: {}'.format(maxage))
    print('gend: {}'.format(gend))
    #to limit memory use
    del demoref
    
    claimdata=subsetByDemographics(claimdata,minage,maxage,gend)
    servdata=subsetByDemographics(servdata,minage,maxage,gend)
    if dexadata is not None:
        dexadata=subsetByDemographics(dexadata,minage,maxage,gend)

    # limit the reference df's to just this metric
    servref=servref[servref['key']==metric]
    indicref=indicref[indicref['key']==metric]
    redflagref=redflagref[redflagref['key']==metric]
            
    
    
    
#=================================================================================================================    
#=================================================================================================================    
#======== INITIATE MAPPING FORWARD AND BACKWARD OF RELEVANT DATAFRAMES   =========================================    
#=================================================================================================================    
#=================================================================================================================    
    if uniqueflagx == False:
    ## NORMAL block of code for running intergenerational assessments (9->10 and 10->9)
    ## First part of block is for updating ref-9 to ICD10 basis
        if (rootICD_gx == 9 and sourcegemdict is not None):
            # update all reference tables with appropriate dictionary (both 1:1 and 1:many dictionaries should work)
            """preopreftab doesn't have ICD codes (?initial impression)"""
             #######################################################################
             
            indHCC= indicref[indicref['class']=='HCC']
            indCCS= indicref[indicref['class']=='CCS']
            indICD= indicref[indicref['class']=='ICD']
            s_indICD= indICD[indICD.startWith==1]
            f_indICD= indICD[indICD.startWith!=1]
            indother= indicref[~indicref['class'].isin(['HCC','CCS','ICD'])]
            
            redHCC= redflagref[redflagref['class']=='HCC']
            redCCS= redflagref[redflagref['class']=='CCS']
            redICD= redflagref[redflagref['class']=='ICD']
            s_redICD= redICD[redICD.startWith==1]
            f_redICD= redICD[redICD.startWith!=1]
            redother= redflagref[~redflagref['class'].isin(['HCC','CCS','ICD'])]
            
            ######
            # servref broken into [servICD, servother] --> then, servICD broken into [s_servICDnf, f_servICDnf, servICD_f]
            servICD = servref[servref['class']=='ICD']
            servICDnf = servICD[servICD.key!='feed']
            servICD_f = servICD[servICD.key=='feed']
            s_servICDnf = servICDnf[servICDnf.startWith==1]
            f_servICDnf = servICDnf[servICDnf.startWith!=1]
            servother= servref[servref['class']!='ICD']
            
            # need to map the serv ICDnf forward
            print('references unpacked')
            print('variable a, (target ICD generation), is {}'.format(a))
            print('columns for dictionary')
            print(sourcegemdict.columns.values)
            print('______________________')
            
            
            ##### BEGINNING TO STEM GEM #####
            ishortest=0
            ilongest=0
            rshortest=0
            rlongest=0
            sshortest=0
            slongest=0
            
            try:
                ishortest= min(s_indICD['subcode'].str.len())
                ilongest=  max(s_indICD['subcode'].str.len())
            except:
                pass
            try:    
                rshortest= min(s_redICD['subcode'].str.len())
                rlongest=  max(s_redICD['subcode'].str.len())
            except:
                pass
            try:    
                sshortest= min(s_servICDnf['code'].str.len())
                slongest=  max(s_servICDnf['code'].str.len())
            except:
                pass
            
            # indications reference mapping
            if ilongest!=0:
                iholder=[]
                for y in range(ishortest, ilongest+1):
                    print(y)
                    gem_short=sourcegemdict.loc[sourcegemdict['ICD{}_subcode'.format(rootICD_gx)].apply(lambda x: x[:y]).isin(s_indICD.subcode),:]
                    gem_short.loc[:,'first']=gem_short.loc[:,'ICD{}_subcode'.format(rootICD_gx)].str[:y]
                    gem_short.rename(columns={'ICD{}_subcode'.format(rootICD_gx):'subcode_todrop',
                                                        'first':'ICD{}_subcode'.format(rootICD_gx)}, inplace=True)
                    gem_short=gem_short.drop(['subcode_todrop'], axis=1, inplace=False)
                    iholder.append(gem_short)                                                
                igem_stemmed= pd.concat(iholder)
                
                s_indICD = s_indICD.merge(igem_stemmed[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='subcode',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
                s_indICD.rename(columns={'subcode':'subcode_defunct',
                               'ICD{}_subcode'.format(a):'subcode'},inplace=True)
                               
            # redflags reference mapping        
            if rlongest!=0:
                rholder=[]
                for y in range(rshortest, rlongest+1):
                    print(y)
                    gem_short=sourcegemdict.loc[sourcegemdict['ICD{}_subcode'.format(rootICD_gx)].apply(lambda x: x[:y]).isin(s_redICD.subcode),:]
                    gem_short.loc[:,'first']=gem_short.loc[:,'ICD{}_subcode'.format(rootICD_gx)].str[:y]
                    gem_short.rename(columns={'ICD{}_subcode'.format(rootICD_gx):'subcode_todrop',
                                                        'first':'ICD{}_subcode'.format(rootICD_gx)}, inplace=True)
                    gem_short=gem_short.drop(['subcode_todrop'], axis=1, inplace=False)
                    rholder.append(gem_short)                                                
                rgem_stemmed= pd.concat(rholder)
                
                s_redICD = s_redICD.merge(rgem_stemmed[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='subcode',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
                s_redICD.rename(columns={'subcode':'subcode_defunct',
                                   'ICD{}_subcode'.format(a):'subcode'},inplace=True)
            
            # services reference mapping
            if slongest!=0:
                sholder=[]
                for y in range(sshortest,slongest+1):
                    print(y)
                    #collect the root ICD stems that are included in the s_servICDnf dataframe
                    gem_short=sourcegemdict.loc[sourcegemdict['ICD{}_subcode'.format(rootICD_gx)].apply(lambda x: x[:y]).isin(s_servICDnf.code),:]
                    # truncate those codes to the appropriate stem length
                    gem_short.loc[:,'first']=gem_short.loc[:,'ICD{}_subcode'.format(rootICD_gx)].str[:y]
                    # match the column names
                    gem_short.rename(columns={'ICD{}_subcode'.format(rootICD_gx):'subcode_todrop',
                                                        'first':'ICD{}_subcode'.format(rootICD_gx)}, inplace=True)
                    gem_short=gem_short.drop(['subcode_todrop'], axis=1, inplace=False)
                    # build the codes into a local ref dataframe
                    sholder.append(gem_short)                                                
                sgem_stemmed= pd.concat(sholder)            
                
                # notice the abnormal column name for servref 'code' instead of 'subcode'
                s_servICDnf = s_servICDnf.merge(sgem_stemmed[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='code',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
                s_servICDnf.rename(columns={'code':'code_defunct',
                                   'ICD{}_subcode'.format(a):'code'},inplace=True)
            
            ######################################################################
            ### Map the none stemmed (aka startWith==0) to other generation
            
            f_indICD = f_indICD.merge(sourcegemdict[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='subcode',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
            
            
            f_redICD = f_redICD.merge(sourcegemdict[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='subcode',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
            
            f_servICDnf = f_servICDnf.merge(sourcegemdict[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='code',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
                                                    
            for x in [f_indICD, f_redICD]:
                print('x.merge columns:')
                x.rename(columns={'subcode':'subcode_defunct',
                                   'ICD{}_subcode'.format(a):'subcode'},inplace=True)
                x.dropna(subset=['subcode'],inplace=True)       

            
            f_servICDnf.rename(columns={'code':'code_defunct',
                                        'ICD{}_subcode'.format(a):'code'}, inplace=True)
            f_servICDnf.dropna(subset=['code'],inplace=True)

            print('post renaming')                  
                
            indHCC = indHCC.merge(sourcegemdict[['hcclev{}'.format(a),
                                           'hcclev{}'.format(rootICD_gx)]],
                                            left_on='subcode', 
                                            right_on='hcclev{}'.format(rootICD_gx),
                                            how='left')   
            redHCC = redHCC.merge(sourcegemdict[['hcclev{}'.format(a),
                                           'hcclev{}'.format(rootICD_gx)]],
                                            left_on='subcode', 
                                            right_on='hcclev{}'.format(rootICD_gx),
                                            how='left')
            for x in [indHCC, redHCC]:
                print('afterHCC merges:')
                print(x.columns.values)   
            
            indCCS = indCCS.merge(sourcegemdict[['ccslev{}'.format(a),
                                            'ccslev{}'.format(rootICD_gx)]],
                                            left_on='subcode',
                                            right_on='ccslev{}'.format(rootICD_gx),
                                            how='left')
            redCCS = redCCS.merge(sourcegemdict[['ccslev{}'.format(a),
                                            'ccslev{}'.format(rootICD_gx)]],
                                            left_on='subcode',
                                            right_on='ccslev{}'.format(rootICD_gx),
                                            how='left')
            for x in [indCCS, redCCS]:
                print('afterCCS merges:')
                print(x.columns.values)   
                
                                            
            indCCS.rename(columns={'subcode':'subcode_defunct',
                                   'ccslev{}'.format(a):'subcode'},inplace=True)
            indHCC.rename(columns={'subcode':'subcode_defunct',
                                   'hcclev{}'.format(a):'subcode'},inplace=True)
            redCCS.rename(columns={'subcode':'subcode_defunct',
                                   'ccslev{}'.format(a):'subcode'},inplace=True)
            redHCC.rename(columns={'subcode':'subcode_defunct',
                                   'hcclev{}'.format(a):'subcode'},inplace=True)
            # ===================================================
            #######################################################################
            ############ New structure July 12 evening
            if metric == 'feed':
                if procgem_dict is not None:
                    servICD_p = servICD_f[servICD_f.code.str[-2:]=='_p']
                    print('procedure only ICDs')
                    print(servICD_p)
                    servICD_p['code']=servICD_p.code.str[:-2]
                    print(servICD_p)
                    servICD_p = servICD_p.merge(procgem_dict[['ICD{}_subcode'.format(a),'ICD{}_subcode'.format(rootICD_gx)]],
                                                                                        left_on='code',
                                                                                        right_on='ICD{}_subcode'.format(rootICD_gx),
                                                                                        how='left')
                    
                    servICD_p['code'].update(servICD_p['ICD{}_subcode'.format(a)])   
                    servICD_p   = servICD_p[[  'class' ,'code' ,'key' ,'label' ,'startWith']]
                else:
                    pass
                servICD_d = servICD_f[servICD_f.code.str[-2:]!='_p']
                s_servICD_d = servICD_d[servICD_d.startWith==1]
                f_servICD_d = servICD_d[servICD_d.startWith!=1]
                    
                    
                    
                f_servICD_d = f_servICD_d.merge(sourcegemdict[['ICD{}_subcode'.format(a),
                                                    'ICD{}_subcode'.format(rootICD_gx)]],
                                                    left_on='code',
                                                    right_on='ICD{}_subcode'.format(rootICD_gx),
                                                    how='left')
                    
                f_servICD_d.rename(columns={'code':'code_defunct',
                                        'ICD{}_subcode'.format(a):'code'}, inplace=True)
                f_servICD_d.dropna(subset=['code'],inplace=True)
                    # this should only update those columns where ICD{}_subcode is not none, thus CPTs should be safe
                    
                sshortest_d = 0
                slongest_d  = 0
                try:    
                    sshortest_d= min(s_servICD_d['code'].str.len())
                    slongest_d=  max(s_servICD_d['code'].str.len())
                except:
                    pass
                if slongest_d!=0:
                    sholder_d=[]
                    for y in range(sshortest_d,slongest_d+1):
                        print(y)
                        #collect the root ICD stems that are included in the s_servICDnf dataframe
                        gem_short=sourcegemdict.loc[sourcegemdict['ICD{}_subcode'.format(rootICD_gx)].apply(lambda x: x[:y]).isin(s_servICD_d.code),:]
                        # truncate those codes to the appropriate stem length
                        gem_short.loc[:,'first']=gem_short.loc[:,'ICD{}_subcode'.format(rootICD_gx)].str[:y]
                        # match the column names
                        gem_short.rename(columns={'ICD{}_subcode'.format(rootICD_gx):'subcode_todrop',
                                                            'first':'ICD{}_subcode'.format(rootICD_gx)}, inplace=True)
                        gem_short=gem_short.drop(['subcode_todrop'], axis=1, inplace=False)
                        # build the codes into a local ref dataframe
                        sholder_d.append(gem_short)                                                
                    sgem_stemmed_d= pd.concat(sholder_d)            
            
                    # notice the abnormal column name for servref 'code' instead of 'subcode'
                    s_servICD_d = s_servICD_d.merge(sgem_stemmed_d[['ICD{}_subcode'.format(a),
                                                'ICD{}_subcode'.format(rootICD_gx)]],
                                                left_on='code',
                                                right_on='ICD{}_subcode'.format(rootICD_gx),
                                                how='left')
                    s_servICD_d.rename(columns={'code':'code_defunct',
                               'ICD{}_subcode'.format(a):'code'},inplace=True)
                
                
                #####
                s_servICD_d = s_servICD_d[['class' ,'code' ,'key' ,'label' ,'startWith']]
                f_servICD_d = f_servICD_d[['class' ,'code' ,'key' ,'label' ,'startWith']]
                if procgem_dict is not None:    
                    #Remember, from servref[key=='feed'] --> [servref_d, servICD_p] from servref_d -->  [s_servICD_d, f_servICD_d, servother_d]
                    servICD_f= pd.concat([servICD_p, s_servICD_d, f_servICD_d])
                else:
                    print("New approach, ignoring that certain GEMs (bestMap9) lack ICD9 Procedure codes for feeding tube placement to ICD10, due to absent equiv-map.")
                    servICD_f= pd.concat([s_servICD_d, f_servICD_d])
            #========================================== 
            
            # Reconstitute the DataFrames
            indICD=pd.concat([s_indICD, f_indICD])
            redICD=pd.concat([s_redICD, f_redICD])
            
            i_con = pd.concat([ indHCC,
                                indCCS,
                                indICD,
                                indother])
            r_con = pd.concat([ redHCC,
                                redCCS,
                                redICD,
                                redother])
                                
            s_con = pd.concat([servother,
                                servICD_f, 
                                s_servICDnf, 
                                f_servICDnf])           
            
            
            print('indicref repacked columns: {}'.format(len(i_con)))
            print(i_con.columns.values)
            print('redflagref repacked columns {}:'.format(len(r_con)))
            print(r_con.columns.values)
            print('servref repacked columns: {}'.format(len(s_con)))
            print(s_con.columns.values)
            
            indicref=i_con
            redflagref=r_con
            servref = s_con
            
            
            #to limit memory use
            del indHCC
            del indCCS
            del indICD
            del indother
            del redHCC
            del redCCS
            del redICD
            del redother
            del s_servICDnf
            del f_servICDnf
            del servICD_f
            del servother
            del i_con
            del r_con
            del s_con
            #######################################################################
            ##  End of ICD9->10 conversions
            #to limit memory use
            del sourcegemdict
            
            print(indicref.head(3))
            print('now reds')
            print(redflagref.head(3))
        ####################################################################################################################
        ####################################################################################################################    
        elif (rootICD_gx == 10 and sourcegemdict is not None):
        #                                                                                                                  #
        ####################################################################################################################
        #       Second part of 'if' branch (this part is converting 10-claims to 9 for comparison with ref-9 codes
        ###     With new ICD codes, merge on ICD codes to map to ICD{a}, HCC{a}, CCS{a} with dictionaries to get new mappings
            if metric == 'feed':
                print("New implementation ignores that certain GEMs lack feeding tube placement mappings because no ICD-procedural codes are in the actual patient data")
            else:
                pass
    
            claimdata=claimdata.merge(sourcegemdict[['ICD{}_subcode'.format(a),
                                                      'ICD{}_subcode'.format(rootICD_gx),
                                                      'hcclev{}'.format(a),
                                                      'ccslev{}'.format(a)]],
                                                     left_on='ICD{}_subcode'.format(rootICD_gx),
                                                     right_on='ICD{}_subcode'.format(rootICD_gx),
                                                     how='left')                                                
            claimdata.rename(columns={'ICD{}_subcode_x'.format(a):'ICD{}_subcode_ignored'.format(a),
                                      'ICD{}_subcode_y'.format(a):'ICD{}_subcode'.format(a),
                                      'hcclev':'hcclev_ignored',
                                      'ccslev':'ccslev_ignored',
                                      'hcclev{}'.format(a):'hcclev',
                                      'ccslev{}'.format(a):'ccslev'},inplace=True)
            #######################################################################
            claimdata.dropna(subset=['ICD{}_subcode'.format(a)],inplace=True)
            
            #to limit memory use
            del sourcegemdict
        elif sourcegemdict is None:
            pass
        else:
            raise KeyError("<rootICD_gx> argument, {}: is inappropriate for current design of code.".format(rootICD_gx))

    elif uniqueflagx==True:
        ##################################################
        ## to perform ICD10 : ICD10 assessments
        #  
        if indicref['class'].str.contains('HCC').any():
            raise ValueError('The algorithm for 10 to 10 was not built for HCC codes in the reference tables')
        elif redflagref['class'].str.contains('HCC').any():
           raise ValueError('The algorithm for 10 to 10 was not built for HCC codes in the reference tables')
        elif indicref['class'].str.contains('CCS').any():
            raise ValueError('The algorithm for 10 to 10 was not built for CCS codes in the reference tables')
        elif redflagref['class'].str.contains('CCS').any():
           raise ValueError('The algorithm for 10 to 10 was not built for CCS codes in the reference tables')
        else:
            pass
        
        claimdata.loc[:,'hcclev']=0
        claimdata.loc[:, 'ccslev']=0
        ##################################################
        
#=================================================================================================================
#=================================================================================================================
#=================================================================================================================
#== SECOND PHASE OF THE ALGORITHM ================================================================================
#=================================================================================================================
#=================================================================================================================
#=================================================================================================================
#=================================================================================================================
    print('dumping null mapped (sub)codes')
    indicref = indicref[~indicref.subcode.isnull()]
    redflagref = redflagref[~redflagref.subcode.isnull()]
    servref = servref[~servref.code.isnull()]
    indicref['length']=indicref['subcode'].apply(lambda x: len(x))
    redflagref['length']=redflagref['subcode'].apply(lambda x: len(x))
    servref['length']=servref['code'].apply(lambda x: len(x))
    indicref =   indicref[indicref['length']>0]
    redflagref = redflagref[redflagref['length']>0]
    servref =    servref[servref['length']>0]
    print('i{},r{},s{}'.format(len(indicref),len(redflagref),len(servref)))
    
    # DUMP unneeded records for those records where denominator is dependent on ICD code (migraine/dementia/lbp/bph dx)
    # Keep in mind that the demented pts / migraine pts use stemmed ICD definitions, which need mapping done above
    if metric in ['psyc','feed','narc','lbp','bph']:    
        print('entered dx-tied loop')
        #####################################################################################################################
        # Need to shed the cases of CPT/drug admin that are not used for diagnosis specific pts (demented/migraine/LUTS/LBP)
        t_claim = claimdata[['MRN','ICD{}_subcode'.format(a), 'EVT_DATE']]
        print(t_claim.head(2))
        # so, create list of MRNs with relevant diagnoses (demented/migraine/lowbackpain/bph)
        xt_claim = list(t_claim.loc[t_claim['ICD{}_subcode'.format(a)].isin(indicref.subcode[indicref['startWith']==0]),'MRN'])
        
        if metric=='feed':
            pass
        else:
            df_t_claim = t_claim.loc[t_claim['ICD{}_subcode'.format(a)].isin(indicref.subcode[indicref['startWith']==0]),['MRN','EVT_DATE']]
            print('df_t_claim has been created')
        
        # don't forget to accomodate the startWith diagnoses
        refCodes=list(indicref.loc[indicref.startWith==1,'subcode'])
        print('refCodes')
        print(refCodes[:5])
        beany=0
        for i in refCodes:      
                print('going through startWith refCodes')
                print(beany)
                beany+=1
                xt_claim=np.append(xt_claim, list(t_claim.loc[t_claim['ICD{}_subcode'.format(a)].str.startswith(i), 'MRN']))
                if metric =='feed':
                    pass
                else:
                    # capture the MRNs and associated dates for the rows with claim codes that are in indications list
                    df_t_claim = pd.concat([df_t_claim, 
                                            t_claim.loc[t_claim['ICD{}_subcode'.format(a)].str.startswith(i), ['MRN','EVT_DATE']]])
                    
        #to limit memory use
        del t_claim        
        print(len(servdata))
        # final list of all MRNs with relevant dx codes for the denominator:
        xt_claim= list(np.unique(xt_claim))
        print('list of MRNs with needed dx')
        ##### Restrict the datasets to MRNs in question 
        if metric =='feed':
            servdata=servdata[servdata.MRN.isin(xt_claim)]
        else:
            df_t_claim.rename(columns={'EVT_DATE':'tEVT_DATE'}, inplace=True)
            df_t_claim.sort_values(['MRN','tEVT_DATE'],ascending=True, inplace=True, na_position='last') 
            # for each individual MRN, keep only the index/first date of a relevant code (ie, date of first record of migraine)
            df_t_claim.drop_duplicates(['MRN','tEVT_DATE'], keep='first', inplace='True')
            print('df_t_claim columns')
            print(df_t_claim.columns.values)
            print('servdata columns')
            print(servdata.columns.values)
            # use inner merge to keep only servdata records of MRNs that are in tailored claims df
            servdata=servdata.merge(df_t_claim, on='MRN', how='inner')
            del df_t_claim
            print('empty cells in tEVT_DATE:')
            print(servdata[servdata.tEVT_DATE.isnull()])
            print('-------------------------')
            #
            # use date criteria to only keep records of services provided after the first relevant claim (claimdate=tEVT_DATE)
            print('implementing time restriction')
            servdata = servdata[servdata.EVT_DATE>=servdata.tEVT_DATE]
        # 
        claimdata= claimdata[claimdata.MRN.isin(xt_claim)]
        print('after xt_claim')
        print('length serv {}'.format(len(servdata)))
        print('length claim {}'.format(len(claimdata)))
        try:
            second_data = second_data[second_data.MRN.isin(xt_claim)]
        except:
            pass
    else:
        pass
    #####################################################################################
    #####################################################################################
    refkey = pd.concat([redflagref,indicref])
    servref['subcode']=servref.code
    allcodes = pd.concat([servref,refkey])
    ############################################### July 8, 2017
    allcodes_nos = allcodes[allcodes.startWith!=1]
    allcodes_s = allcodes[allcodes.startWith==1]
    print('allcodes_s:')
    print(allcodes_s.groupby('class').count())
    allcodes_nos = list(allcodes_nos['subcode'])
    allcodes_s   = list(allcodes_s['subcode'])
    ################################################ July 8
    print('length before extracting special tables {}'.format(len(servdata)))
    print(servdata.head(1))
    
    # PREPPING INDIVIDUAL METRICS FOR DENOMINATOR and PREPROCESSING
    if metric in ['nonpreop','catpreop']:
        print('creating rCODE column in preop metrics')
        # these metrics need indirect CPTs carried into the preprocessingclaims method
        servdata['rCODE']=0
    
    elif metric in metrics_needing_drugs_as_primary_code_but_still_use_cpts: #narcs and antipsychotics
        # second_data has the relevant cpts
        print('metrics_needing_drugs ...')
        second_data= second_data[second_data.rCODE.isin(list(refkey.loc[refkey['class'].isin(['CPT','DRUG']),'subcode']))]
        
        earlydenom = servdata.rename(columns={'EVT_DATE':'TEST_DATE','CODE':'TEST_CODE'},inplace=False)
        earlydenom['TEST_DATE_a']=earlydenom['TEST_DATE'].values.astype('datetime64[D]')
        reportStartDatex=earlydenom.sort_values('TEST_DATE',ascending=True)['TEST_DATE'].iloc[0]
        earlydenom = earlydenom[earlydenom.TEST_DATE_a>=window_date]
        # Set the denominator
        #
        earlydenom = add_month(earlydenom,'TEST_DATE') 
        output_denominator=earlydenom.drop_duplicates(subset=['MRN','TEST_DATE_month']).sort_values(['MRN','TEST_DATE_a'])

        # cut down the remaining records, to only keep those who actually received the medicine in question. 
        servdata=servdata[servdata['CODE'].isin(servref.code)]   

        # for memory needs, drop all the ICD codes that are irrelevant 
        #   - this MESSES UP the inner merges that come later in preprocessingclaims()
        #   - hence these metrics need curation. only use for those metrics that don't demand a specific diagnosis/ICD in the chart (ie, not demented/migraines)
        print('claimdata columns:')
        print(claimdata.columns.values)
        ##################### July 8       
        claimdata = df_claimtrimmer(claimdata, a, allcodes_nos, allcodes_s)
        sizeflag = 1
        #######################
        print('claimdata after code restriction')
        print(claimdata.head(2))
        # meds metrics are now ready for preprocessingclaims()
    
    elif metric in ['bph','lbp']:
        earlydenom = servdata.rename(columns={'EVT_DATE':'TEST_DATE','CODE':'TEST_CODE'},inplace=False)
        earlydenom['TEST_DATE_a']=earlydenom['TEST_DATE'].values.astype('datetime64[D]')

        reportStartDatex=earlydenom.sort_values('TEST_DATE',ascending=True)['TEST_DATE'].iloc[0]
        earlydenom = earlydenom[earlydenom.TEST_DATE_a>=window_date]
        # Set the denominator
        #### 
        # should we get rid of TEST_CODE/TEST_DATE_a? 
#        output_denominator=earlydenom.drop_duplicates(subset=['MRN','TEST_DATE_a','TEST_CODE']).sort_values(['MRN','TEST_DATE_a'])
        earlydenom = add_month(earlydenom,'TEST_DATE')
        output_denominator=earlydenom.drop_duplicates(subset=['MRN','TEST_DATE_month']).sort_values(['MRN','TEST_DATE_a'])
        # cut down the remaining records, to only keep those who actually received the imaging in question. 
        servdata=servdata[servdata['CODE'].isin(servref.code)].sort_values('EVT_DATE',ascending=True)  
        servdata['rCODE']=0 
        print('servdata after code restriction')
        print(servdata.head(2))
        # bph and lbp are prepped for preprocessingclaims()
            
    elif metric in ['dexa','vitd']:
        # keep track of CPT codes that are used in redflags/indications
        # copy out only relevant CPT codes (redflag/indications) from main servdata dataframe
        print('in the dexa/vitd servdat_x chunk')
        servdat_x = servdata[['EVT_DATE','CODE','MRN']]
        servdat_x.rename(columns={'EVT_DATE':'rEVT_DATE','CODE':'rCODE', 'MRN':'rMRN'}, inplace=True)
        # this may be wrong
        servdat_x = servdat_x[servdat_x.rCODE.isin(list(refkey.loc[refkey['class'].isin(['CPT','DRUG']),'subcode']))]
        servdata=servdata[servdata['CODE'].isin(servref.code)]                

    elif metric == 'feed':
        ############ New structure July 12 evening 
        if procgem_dict is not None:  
            # feed metric needs this because it relies on procedural ICDs (in only one GEM implementation): 
            # first, get MRNs with relevant ICD-procedure codes
            claimfeedproc = claimdata[claimdata['ICD{}_subcode'.format(a)].isin(servref.code)]
            sepMRNclaim = list(claimdata.loc[claimdata['ICD{}_subcode'.format(a)].isin(servref.code),'MRN'])
            sepMRNclaim = np.unique(sepMRNclaim)
            print('number MRNs w/ feeding ICDs: {}'.format(len(sepMRNclaim)))
        else:
            print('skipped feeding tube procedure ICDs given specific GEM implementation, and lack of procedural mappings')
        # at this point, servdata has all service records of MRNs with dementia, without regard to feeding tube placement
        servdata.rename(columns={'EVT_DATE':'TEST_DATE','CODE':'TEST_CODE'},inplace=True)
        servdata['TEST_DATE_a']=servdata['TEST_DATE'].values.astype('datetime64[D]')
        reportStartDatex=servdata.sort_values('TEST_DATE',ascending=True)['TEST_DATE'].iloc[0]
        earlydenom = servdata
        earlydenom = earlydenom[earlydenom.TEST_DATE_a>=window_date]
        # Set the denominator
        earlydenom = add_month(earlydenom,'TEST_DATE')
        output_denominator=earlydenom.drop_duplicates(subset=['MRN','TEST_DATE_month']).sort_values(['MRN','TEST_DATE_a'])
        if procgem_dict is not None: 
                # now, select the MRNs w/ dementia that have feeding tube by ICD records
            in_serv = servdata[servdata.MRN.isin(sepMRNclaim)]
            claimfeedproc = claimfeedproc[['MRN','EVT_DATE','ICD{}_subcode'.format(a),'hcclev','ccslev']]
            claimfeedproc.rename(columns={'EVT_DATE':'CLAIM_DATE',
                                        'ICD{}_subcode'.format(a):'ICD{}_proccode'.format(a)}, inplace=True)
            claimfeedproc = claimfeedproc[claimfeedproc.CLAIM_DATE>=window_date]    
            in_serv = in_serv.merge(claimfeedproc, on='MRN', how='inner')
            # limit the merge to single row per ICD{}_proccode
            in_serv = in_serv[in_serv['TEST_DATE_a']>=window_date]
            in_serv = in_serv.drop_duplicates(['MRN', 'CLAIM_DATE', 'ICD{}_proccode'.format(a)])
            in_serv['based_on'] = 'ICD'
            out_serv = servdata[~servdata.MRN.isin(sepMRNclaim)]
        else:
            out_serv = servdata
        # of those not included based on ICD-procedure coding, which ones have CPT for feeding tube placement?

        out_serv = out_serv[out_serv['TEST_CODE'].isin(servref.code)] 
        out_serv = out_serv[out_serv['TEST_DATE_a']>=window_date]
        out_serv = out_serv.drop_duplicates(['MRN','TEST_DATE_a','TEST_CODE']).sort_values(['MRN','TEST_DATE_a'], ascending=True)
        out_serv['based_on'] = 'CPT'
        if procgem_dict is not None: 
            # this is a df of all MRNs with dementia who've received a feeding tube:
            servdata = pd.concat([in_serv,out_serv])
            servdata= add_month(servdata,'TEST_DATE')
            servdata=servdata.drop_duplicates(['MRN','TEST_DATE_a','TEST_CODE','CLAIM_DATE','ICD{}_proccode'.format(a)]).sort_values(['MRN','TEST_DATE_a'], ascending=True)
        
        else:
            servdata = out_serv
            servdata= add_month(servdata,'TEST_DATE')
            servdata=servdata.drop_duplicates(['MRN','TEST_DATE_a','TEST_CODE']).sort_values(['MRN','TEST_DATE_a'], ascending=True)
        
        print('early completion')        
        output_numerator=servdata
        print('numerator exists, 1 row:')
        print(output_numerator.head(1))
        print('denominator exists, 2 rows:')
        output = dict(numerator=output_numerator, denominator =output_denominator)
        print('reached end of cycle.')
        print('feed.keys() {}'.format(output.keys()))
        print(len(output['numerator']))
        print(len(output['denominator']))
        return (output)
        
    elif metric =='card':
        print('card chunk')
        sizeflag=1
        servdata=servdata[servdata['CODE'].isin(servref.code)].sort_values('EVT_DATE',ascending=True)  
        servdata['rCODE']=0
        

    elif tempsort_day is False:
        # drop the CPTs that aren't explicitly tied to the metric, for metrics that don't use CPTs in redflags/indications
        print('in tempsort_day False chunk')
        servdata=servdata[servdata['CODE'].isin(servref.code)]
        servdata['rCODE']=0
        
    else:      
        print('length: {}'.format(len(servdata)))
        print(servdata.head(1))
        print('last else chunk entered')
        
        # drop the CPTs that aren't explicitly tied to the metric, for metrics that don't use CPTs in redflags/indications
        servdata=servdata[servdata['CODE'].isin(servref.code)].sort_values('EVT_DATE',ascending=True)  
        servdata['rCODE']=0
        
    print('length of dataframe entering preprocessing:')
    print(len(servdata))

    # =====================================================================================================
    # =====================================================================================================
    # Run the main preprocessing block
    # =====================================================================================================
    # =====================================================================================================
    claimdata = claimdata[['MRN','EVT_DATE','ICD{}_subcode'.format(a),'hcclev','ccslev']]
    if sizeflag == 0:
        try:
            print('attempting sizeflag=0')
            servdata=preprocessingclaims(servdata,claimdata, servref, preopreftab, admsdata, metric, a, tempsort_day, metrics_needing_drugs_as_primary_code_but_still_use_cpts, sizeflag)
        except:
            print('sizeflag exception raised, trying sizeflag =1')
            sizeflag =1 if tempsort_day is False else 0
            ##################################### July 8 17
            claimdata = df_claimtrimmer(claimdata, a, allcodes_nos, allcodes_s)
            #######################
            servdata=preprocessingclaims(servdata,claimdata, servref, preopreftab, admsdata, metric, a, tempsort_day, metrics_needing_drugs_as_primary_code_but_still_use_cpts, sizeflag)
    else:
        print('using sizeflag=1')
        ##################################### July 8 17
        claimdata = df_claimtrimmer(claimdata, a, allcodes_nos, allcodes_s)
        #######################
        servdata=preprocessingclaims(servdata,claimdata, servref, preopreftab, admsdata, metric, a, tempsort_day, metrics_needing_drugs_as_primary_code_but_still_use_cpts, sizeflag)

    #print('FYI the metrics included in drugs with cpts: {}'.format(metrics_needing_drugs_as_primary_code_but_still_use_cpts))
    
    if metric=='narc':
        ### most recent version:
        print('narc before adms')
        print(servdata.head(2))
        # it is ok to drop these at this point, because denominator set above (unlike preop metrics)
        servdata=servdata[servdata.adms_flag==False]    
        print('admission flag just implemented')
        try:
            print(output_denominator.head(2))
        except:
            pass
    else:
        pass
    
    if metric in metrics_needing_drugs_as_primary_code_but_still_use_cpts:
        print('in second_data chunk')
        print(servdata.head(2))
        print(second_data.head(3))
        servdata=servdata.merge(second_data, left_on='MRN', right_on='rMRN', how='left')
        del second_data
        print('length after second_data {}'.format(len(servdata)))
        servdata.rEVT_DATE.fillna(np.datetime64('1900-01-01'), inplace=True)
        servdata.rCODE.fillna(0, inplace=True)
        print('before TEST_DATE>=rEVT_DATE {}'.format(len(servdata)))
        servdata=servdata[servdata.TEST_DATE>=servdata.rEVT_DATE]
        print('after TEST_DATE>= {}'.format(len(servdata)))
        servdata['dys_difffrom_rCODE']=servdata.TEST_DATE.sub(servdata.rEVT_DATE)/np.timedelta64(1,'D')
        servdata.drop(['rEVT_DATE'], axis=1, inplace=True)
        print('leaving second_data chunk {}'.format(len(servdata)))
        
    elif metric in ['dexa','vitd']:
        servdata=servdata.merge(servdat_x[['rEVT_DATE','rMRN','rCODE']], left_on='MRN', right_on='rMRN', how='left')
        #to limit memory use
        del servdat_x
        servdata.rEVT_DATE.fillna(np.datetime64('1900-01-01'), inplace=True)
        servdata.rCODE.fillna(0, inplace=True)
        # this assumes that only services before service in question should be considered
        servdata=servdata[servdata.TEST_DATE>=servdata.rEVT_DATE]
        servdata['dys_difffrom_rCODE']=servdata.TEST_DATE.sub(servdata.rEVT_DATE)/np.timedelta64(1,'D')
        print('length of servdata with cpts merged')
        print(len(servdata))
        servdata.drop(['rEVT_DATE'], axis=1, inplace=True)
    else:
        pass

    ##################################################################################################
    ### Post - preprocessing                                                                        ##
    ##################################################################################################
    if metric in ['nonpreop','catpreop']:
        #only keep the records of testing done w/in tempsort_day(s) of surgery.
        servdata['daysBeforeSurg']=servdata.SURG_DATE.sub(servdata.TEST_DATE) 
        servdata=servdata[(servdata.daysBeforeSurg<=timedelta(tempsort_day))]#
    else:
        pass
    
    if metric in metrics_needing_drugs_as_primary_code_but_still_use_cpts: #narcs and antipsychotics
        pass
    elif metric in ['lbp','bph']:
        pass
    elif metric =='feed':
        raise ValueError('feed metric skipped the main processing chunk incorrectly')
    else:
        print('post preprocessing')
        print(len(servdata))
        reportStartDatex=servdata.sort_values('TEST_DATE',ascending=True)['TEST_DATE'].iloc[0]
        # Set the denominator
        servdata['TEST_DATE_a']=servdata['TEST_DATE'].values.astype('datetime64[D]')
        print('before denominator grouping after full preprocessclaims()')
        print(servdata.columns.values)
        output_denominator= add_month(servdata,'TEST_DATE')
        output_denominator= output_denominator.drop_duplicates(subset=['MRN','TEST_DATE_a','TEST_CODE']).sort_values(['MRN','TEST_DATE_a'])
    try:
        print('again, the output_denom:')
        print(output_denominator.head(2))
    except:
        pass    
# =================================================================================================================  
# Now, determine the numerator (for non-feeding tube metrics)
# =================================================================================================================
    if dexadata is not None:
        print('dexadata is not None')
        try:
            print(output_denominator.head(2))
        except:
            pass
        print(dexadata.head(1))
        #restrict df to only those MRNs with more than one record:
        dexadata=dexadata[dexadata.MRN.duplicated(keep=False)]
        # but get rid of duplicate records on same day
        dexadata=dexadata.sort_values(['MRN','EVT_DATE'],ascending=True).drop_duplicates(['MRN','EVT_DATE'])
        grdex=dexadata.groupby('MRN').apply(lambda x:timeBtwnDex(x,'EVT_DATE'))
        print('grdex: {}'.format(grdex.head(2)))
        grdex=grdex[grdex['since_last']<timedelta(tempsort_day)]
        servdata=servdata[servdata.MRN.isin(grdex['MRN'][grdex['EVT_DATE']>=reportStartDatex])]
        print('servdata.isin(grdex) {}'.format(servdata.head(2)))
        grdex_enc= grdex.dropna(subset=['since_last'], inplace=False)
        print('grdex_enc : {}'.format(grdex_enc.head(2)))
        print('end of dexadata')
        try:
            print(output_denominator.head(2))
        except:
            pass
        #to limit memory use
        del grdex
        servdata=servdata[servdata.ENC_ID.isin(grdex_enc['ENC_ID'])]
        print('servdata after grdex_enc {}'.format(servdata.head(2)))
        #to limit memory use
        del grdex_enc
    else:
        pass
    #REMOVE MRNs Meeting Red Flags, KEEP MRNs using inappropriate indications
    try:
        print('just before exclusions')
        print(servdata.head(2))
        print('...BAR...')
        print(output_denominator.head(2))
    except:
        pass
    ####### new: July 10 2017
    servdata= servdata[servdata.TEST_DATE>=window_date]  
    #########################  
    servdata=keepindications_removeexclusions(metric,servdata,redflagref,'exclude',lookback, tempsort_day, a, uniqueflagx)
    try:
        print('check a')
        print(servdata.head(2))
        print('....BAR....')
        print(output_denominator.head(2))
    except:
        pass
    """
    if len(indicref[indicref['key']==metric])>0:
        servdata=keepindications_removeexclusions(metric,servdata,indicref,'include',lookback, tempsort_day, a, uniqueflagx)
    """    
    print('check b')
    try:
        print(servdata.head(2))
        print('....BAR....')
        print(output_denominator.head(2))
        
    except:
        pass
    if metric in ['lbp','bph']:   # this block of code should come earlier so that rows aren't lost ??
        print('lbp/bph loop started')
        if metric not in ['lbp','bph']:
            print("Caution, parsing of numerator for long-established diagnoses is currently applied, but was only developed for lbp and bph metrics.\n")
        servdata=servdata[(servdata['dys_difftime_test_code']>=0)]
        print('first cut of difftime applied')
        # Of the remaining in the numerator group, get rid of those entries dependent on longstanding dx that precede the tempsort_day period

        #######################################################################
        #######################################################################
        # the marker "2" is used to ensure no confusion with the marker "1"
        print('creating cullmarker')
        servdata['cullmarker']=0
        servdata.loc[servdata['dys_difftime_test_code']>=tempsort_day,'cullmarker']= 2
        servdata_c1 = servdata[servdata.cullmarker==2]
        servdata_c0 = servdata[servdata.cullmarker!=2]
        servdata_c0['cullmarker']=0
        servdata_c1['cullmarker']=0 # reset the longstanding group
        print('culled by difftime')
        # Mark the rows, which have preceding indication (lbp or LUTS complaint) that has been longstanding, with "1"
        #       . These rows are therefore "valid" and not wasteful  ---> drop these rows
        #       . those rows that remain "0"'s remain wasteful
        servdata_c1.loc[servdata_c1.ccslev.astype(str).isin(indicref['subcode'][indicref['class']=='CCS']), 'cullmarker'] =1
        servdata_c1.loc[servdata_c1.hcclev.astype(str).isin(indicref['subcode'][indicref['class']=='HCC']), 'cullmarker'] =1
        servdata_c1.loc[servdata_c1['ICD{}_subcode'.format(a)].isin(indicref['subcode'][indicref['class']=='ICD']),'cullmarker'] =1
        servdata_c1 = servdata_c1[servdata_c1.cullmarker!=1]
        print('dropped longstanding cases')
        servdata= pd.concat([servdata_c0,
                            servdata_c1])
        ##### NEW END END END END
    elif ((metric=='catpreop') or (metric=='nonpreop')):
        ### most recent version:
        servdata=servdata[servdata.adms_flag==False]
    else:
        pass

    # Cut down to unique services and Encounters, don't double count after inner join
    print('check d')
    try:
        print(output_denominator.head(2))
    except:
        pass
    print(servdata.columns.values)
    servdata=servdata.drop_duplicates(['MRN','TEST_DATE_a','TEST_CODE']).sort_values(['MRN','TEST_DATE_a'], ascending=True)
    output_numerator = servdata
    output_numerator = add_month(output_numerator,'TEST_DATE')
    print('check e')
    try:
        print(output_denominator.head(2))
    except:
        pass
    print('keys')
    print(output.keys())
    try:
        print(output_denominator.head(2))
    except:
        pass
    output = dict(numerator=output_numerator, denominator=output_denominator)
    print(output['numerator'].head(2))
    print('...BAR....')
    print(output['denominator'].head(2))
#    output={k:add_month(v,'TEST_DATE') for k,v in output.iteritems() if 'TEST_DATE' in v.columns}

    print('one more time')
    print(output['denominator'].head(2))
    print('reached end of cycle.')
    print(output.keys())
    print(len(output['numerator']))
    print(len(output['denominator']))
       
    return (output)

###############################################################################################

