# Load GEMs
#Load the GEMS equ-map
# perform these processes before reprocessing gems 

# Create dictionaries from map
#       . If ICD9 to ICD10, expansive maps are ok 
#       . If ICD10 to ICD9, need nonexpansive mappings
import numpy as np
import pandas as pd

def createuniquemap(xgem, rootICD_gen, hccrefx, method, output='dict'):
    a=10 if rootICD_gen == 9 else 9
    xgem_1= xgem.groupby(['ICD{}_CODE'.format(rootICD_gen)])['ICD{}_CODE'.format(a)].nunique()
    if max(xgem_1)<=1:
        print("Checked {}.gem: No duplicates to parse.".format(xgem))
        return xgem 
    if method == 'random':
        xgem_1=xgem.groupby('ICD{}_subcode'.format(rootICD_gen)).apply(lambda x: x['ICD{}_subcode'.format(a)].sample(random_state=1873))
        xgem_1=xgem_1.reset_index()
        xgem_out=xgem[xgem.index.isin(xgem_1.level_1)]
    elif (method == 'upcode' or 'downcode'):
        #merge in the HCC levels associated with the destination_ICDgeneration (so for ICD9->ICD10. merge in hcc's for ICD10)
        xgem_1=xgem.merge(hccrefx['{}'.format(a)][['subcode','hcclev']],left_on='ICD{}_subcode'.format(a), right_on='subcode',how='left')
        if method== 'upcode':
            xgem_1i=xgem_1.groupby('ICD{}_subcode'.format(rootICD_gen))['hcclev'].idxmin()
        else:
            xgem_1i=xgem_1.groupby('ICD{}_subcode'.format(rootICD_gen))['hcclev'].idxmax()
        xgem_1i=xgem_1i.reset_index()
        
        xgem_2=xgem_1[xgem_1.index.isin(xgem_1i['hcclev'])]
        xgem_2n=xgem_1[~xgem_1['ICD{}_subcode'.format(rootICD_gen)].isin(xgem_1i['ICD{}_subcode'.format(rootICD_gen)][xgem_1i.hcclev.notnull()])]
        if method == 'upcode':
            xgem_2ni=xgem_2n.groupby('ICD{}_subcode'.format(rootICD_gen))['ICD{}_subcode'.format(a)].max()
        if method == 'downcode':
            xgem_2ni=xgem_2n.groupby('ICD{}_subcode'.format(rootICD_gen))['ICD{}_subcode'.format(a)].min()
        xgem_2ni=xgem_2ni.reset_index()
        # to allow clean inner-merge
        if 'NZ_INSERT_DT' in xgem_2.columns.values:
            xgem_2=xgem_2.drop(['NZ_INSERT_DT','RELATION_GUID'],1)
            xgem_2n=xgem_2n.drop(['NZ_INSERT_DT','RELATION_GUID'],1)
        else:
            xgem_2=xgem_2.drop(['RELATION_GUID'],1)
            xgem_2n=xgem_2n.drop(['RELATION_GUID'],1)
        xgem_2n.drop_duplicates()
        xgem_2n=xgem_2n.merge(xgem_2ni, on=['ICD{}_subcode'.format(a),'ICD{}_subcode'.format(rootICD_gen)], how='inner')
        xgem_out=pd.concat([xgem_2, xgem_2n])        
    else:
        raise KeyError("String selecting chosen method for reducing map to 1:1 pairings is invalid.")
    if output == 'dict':
        xgem_out = xgem_out[['ICD{}_subcode'.format(rootICD_gen),'ICD{}_subcode'.format(a)]]
    elif output == 'table':
        #to reset default output
        output = 'dict'
    return xgem_out


def simplify_xgem(xgem, rootICD_gen, subtype):
    """
    Function: simplify_xgem(xgem, rootICD_gen)
    Takes a gen-equiv map, restricts to CM codes (drops procedural codes),
                           and drops expired maps.
    """
    if subtype == 'd':
        if rootICD_gen == 9:
            xgem1=xgem[xgem.ICD9_TYPE=='ICD_{}_CM_CODE'.format(rootICD_gen)]
        elif rootICD_gen == 10:
            xgem1=xgem[xgem.ICD10_TYPE=='ICD{}CM_CODE'.format(rootICD_gen)]
        else: 
            raise ValueError("Inappropriate ICD9 v. ICD10 flag entered for second argument.")
    elif subtype == 'p':
        if rootICD_gen == 9:
            xgem1=xgem[xgem.ICD9_TYPE=='ICD_{}_PROC_CODE'.format(rootICD_gen)]
        elif rootICD_gen == 10:
            raise ValueError("Unable to verify that input gem was preprocessed correctly")
        else: 
            raise ValueError("Inappropriate ICD9 v. ICD10 flag entered for second argument.")
    else:
        raise ValueError("third argument is invalid.")
    try: 
        xgem1=xgem1[xgem1['MAPPING_EXPIRATION_TIMESTAMP'].isnull()]
    except:
        xgem1=xgem1[xgem1['MAPPING_EXPIRATION_DATE'].isnull()]
    if subtype =='d':
        if len(xgem1.groupby(['ICD{}_CODE'.format(rootICD_gen)])['RELATION_TYPE'].filter(lambda x: len(np.unique(x))>1))>1:
            raise ValueError("This general equiv map has both Approximate and Equal mappings for at least one ICD_{} Code".format(rootICD_gen))
    return xgem1


def seeambiguousmaps(xgem, rootICD_gen):
    xgem1 = simplify_xgem(xgem, rootICD_gen)
    a=10 if rootICD_gen == 9 else 9
    xgem2= xgem1.groupby(['ICD{}_CODE'.format(rootICD_gen)])['ICD{}_CODE'.format(a)].nunique()
    if max(xgem2)<=1:
        return "There are no ambiguous maps"
    return "max map is {} vectors".format(max(xgem2)), xgem1[xgem1['ICD{}_CODE'.format(rootICD_gen)].isin(list(xgem2[xgem2>=2].index))].sort('ICD{}_CODE'.format(rootICD_gen), ascending=True)[['RELATION_TYPE','ICD{}_CODE'.format(rootICD_gen),'ICD{}_CODE'.format(a),'ICD{}_DESC'.format(rootICD_gen),'ICD{}_DESC'.format(a)]]

