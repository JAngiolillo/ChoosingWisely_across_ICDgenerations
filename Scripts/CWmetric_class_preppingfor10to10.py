import numpy as np
import pandas as pd
from chooseallwisely_mod_preppingfor10to10 import chooseallwisely
# this line is out of date:
from report_chooseall import rep_CW

class MetricClass(object):
    """
    ##############################################################
    This first main block (above class methods) outlines the main structures of the MetricClass,
    and records rules to ensure proper implementation.
    **************************************************************
    
    This block of code creates the dictionaries that are called on by individual methods, and shared by multiple class instances"""
    servdata_dict = {'DEFAULT':None}       ## dict 0
    claimdata_dict = {}      ## dict 1
    demodata_dict = {}       ## dict 2
    demoref_dict = {}        ## dict 3
    indicref_dict = {}       ## dict 4
    redflagref_dict = {}     ## dict 5
    servref_dict = {}        ## dict 6
    dexadata_dict = {}       ## dict 7
    preopreftab_dict = {}    ## dict 8
    admsdata_dict = {}       ## dict 9
    pooledgems_dict = {'DEFAULT':None}
    pooledprocgem_dict = {'DEFAULT':None}
#    second_data_dict = {'DEFAULT':None}           

    """class global variables referenced in error checks:"""   
    # createclass creates a lock, to only allow single initiation of underlying class               
    createclass = 0
    # this list denotes the metrics for which the class is built
    metricsbuilt=['lbp', 'bph',
                  'card', 'cerv',
                  'dexa', 'catpreop',
                  'nonpreop', 'vitd', 
                  'psyc', 'feed', 'narc']

    poss_kwargs= ['servdata','dexadata','preopreftab','admsdata','tempsort_day',
                  'lookback','rootICD_genx','gemdict','procgem_dict','uniqueflagx',
                  'second_datax']

    metrics_needing_tempsort_day = ['lbp','bph','dexa',
                                    'narc','catpreop','nonpreop']


    """
    ###############################
    This section includes the class methods for loading data between instances
    *******************************"""    
    @classmethod
    def load_dataframes(cls,
                        c_servdata=None, c_claimdata=None, 
                        c_demodata=None, c_demoref=None,
                        c_indicref=None, c_redflagref=None,
                        c_servref=None, c_dexadata=None,
                        c_preopreftab=None, c_admsdata=None, 
                        c_newindicref=None, c_newredflagref=None,
                        c_newservref=None, c_window_date = None):#, c_seconddata=None):
        if cls.createclass==0:
            cls.servdata_dict['SERVDATA'] = c_servdata          ## dict 0
            cls.claimdata_dict['CLAIMDATA'] = c_claimdata       ## dict 1
            cls.demodata_dict['DEMODATA'] = c_demodata          ## dict 2
            cls.demoref_dict['DEMOREF'] = c_demoref             ## dict 3
            cls.indicref_dict['INDICREF'] = c_indicref          ## dict 4
            cls.redflagref_dict['REDFLAGREF'] = c_redflagref    ## dict 5
            cls.servref_dict['SERVREF'] = c_servref             ## dict 6
            
            # This part is for loading ICD-10 based reference data
            cls.indicref_dict['new_INDICREF'] = c_newindicref   ## dict 4a
            cls.redflagref_dict['new_REDFLAGREF'] = c_newredflagref ## dict 5a
            cls.servref_dict['new_SERVREF'] = c_newservref      ## dict 6a
            
            cls.dexadata_dict['DEXADATA'] = c_dexadata          ## dict 7
            cls.preopreftab_dict['PREOPREFTAB'] = c_preopreftab ## dict 8
            cls.admsdata_dict['ADMSDATA'] = c_admsdata          ## dict 9
            cls.window_DT = c_window_date
#            cls.second_data_dict['second_serv']=c_seconddata
            cls.createclass=1
        else:
            print("Cannot modify Choosing Wisely dataframes again.")

    @classmethod
    def change_newreferences(cls, replacement_indicref, replacement_redflagref, replacement_servref, flagy, in_key=None, rf_key=None, sv_key=None):
        if flagy =='replace':
            cls.indicref_dict['new_INDICREF']=replacement_indicref
            cls.redflagref_dict['new_REDFLAGREF']=replacement_redflagref
            cls.servref_dict['new_SERVREF']=replacement_servref
        elif flagy =='add':
            cls.appendindicref_dict(in_key, replacement_indicref)
            cls.appendredflagref_dict(rf_key, replacement_redflagref)
            cls.appendservref_dict(sv_key, replacement_servref)
        else:
            raise KeyError('There is a problem with the change_newreferences() classmethod implementation. \n Ensure that references are dataframes \n and that in_key, rf_key and sv_key are strings')
        
    @classmethod
    def load_gemdictionaries(cls, pooled_preprocessed_claimdictionaries, pooled_preprocessed_procdictionaries):
        cls.pooledgems_dict.update(pooled_preprocessed_claimdictionaries)
        cls.pooledprocgem_dict.update(pooled_preprocessed_procdictionaries)
            
    """Class methods for altering individual argument dictionaries"""
    
    @classmethod
    def appendservdata_dict(cls, key_label, x_dataframe):
        cls.servdata_dict[key_label]= x_dataframe
        
    @classmethod
    def appendclaimdata_dict(cls, key_label, x_dataframe):
        cls.claimdata_dict[key_label]= x_dataframe    
    
#    @classmethod
#    def appendsecond_data_dict(cls, key_label, x_dataframe):
#        cls.second_data_dict[key_label]= x_dataframe
    
    @classmethod
    def appenddemodata_dict(cls, key_label, x_dataframe):
        cls.demodata_dict[key_label]= x_dataframe
    
    @classmethod
    def appenddemoref_dict(cls, key_label, x_dataframe):
        cls.demoref_dict[key_label]= x_dataframe
    
    @classmethod
    def appendindicref_dict(cls, key_label, x_dataframe):
        cls.indicref_dict[key_label]= x_dataframe
    
    @classmethod
    def appendredflagref_dict(cls, key_label, x_dataframe):
        cls.redflagref_dict[key_label]= x_dataframe
    
    @classmethod
    def appendservref_dict(cls, key_label, x_dataframe):
        cls.servref_dict[key_label]= x_dataframe
    
    @classmethod
    def appenddexadata_dict(cls, key_label, x_dataframe):
        cls.dexadata_dict[key_label]= x_dataframe

    @classmethod
    def appendpreopreftab_dict(cls, key_label, x_dataframe):
        cls.preopreftab_dict[key_label]= x_dataframe
    
    @classmethod
    def appendadmsdata_dict(cls, key_label, x_dataframe):
        cls.admsdata_dict[key_label]= x_dataframe
    
    
    
    
    
    """
    ########################################################################################
    Initialization of each class instance
    ****************************************************************************************
    """

    def __init__(self,metric,**kwargs):
        """This block sets the defaults string arguments for use as keys in the shared dictionaries """
        self._servdata=  'SERVDATA'      #1# # first argument is [zero] -- see below
        self._claimdata= 'CLAIMDATA'    #2#
        self._demodata=  'DEMODATA'      #3#
        self._demoref=   'DEMOREF'        #4#
        self.indicref=  'INDICREF'     #5#
        self.redflagref='REDFLAGREF'  #6#
        self.servref=   'SERVREF'    #7#
        self._dexadata=  'DEXADATA'  #8#
        self._preopreftab='PREOPREFTAB'#9#
        self._admsdata=   'ADMSDATA'   #10

        """This block updates the default keyword arguments as some metrics require """
        self._kwargs=(kwargs)
        for x in self._kwargs:
            if x not in MetricClass.poss_kwargs:
                raise KeyError("keyword argument '{}' is inappropriate for this class.".format(x)) 
        #(here are the standard keyword defaults)
        default_args = {'tempsort_day':True,
                        'lookback':365, 
                        'rootICD_genx':10, 
                        'gemdict':'DEFAULT', 
                        'procgem_dict':'DEFAULT', 
                        'uniqueflagx':False,
                        'second_datax':'DEFAULT'} 
        self.cdefault_args=default_args
        #(actual replacement of defaults by single implementation keyword arguments)
        self.cdefault_args.update(self._kwargs)
        for x in ['servdata', 'dexadata', 'preopreftab', 'admsdata']:
            # this is a loose step that relies on user to only use valid entries - where valid means a key already in cls.x_dict.keys()
            # appears to allow change to change of assigned keys to these dictionaries without using classmethod
            if x in self.cdefault_args.keys():
                print("the code: [ >>>setattr(self, '_' + x, self.cdefault_args[x]) ] is being implemented for {}.".format(x))
                setattr(self, '_' + x, self.cdefault_args[x])
        self.tempsort_day=self.cdefault_args['tempsort_day']    #11#
        self.lookback=self.cdefault_args['lookback']            #12#
        self.rootICD_genx=self.cdefault_args['rootICD_genx']    #13#
        self.gemdict=self.cdefault_args['gemdict']              #14#
        self.procgem_dict=self.cdefault_args['procgem_dict']    #15#
        self.uniqueflagx=self.cdefault_args['uniqueflagx']      #16#
        self.second_datax=self.cdefault_args['second_datax']

        
        
        self._metricreports={} # not yet used
        self._m_counter=0     
        self.metric = metric                                    #0#
        # creates a list of the arguments to be passed into choosingallwisely() method
        #       . as implemented by the run_CW() method.
        self.names_of_args=  [self.metric,                      #0
                              self.servdata(),                  #1
                              self.claimdata(),                 #2
                              self.demodata(),                  #3
                              self.demoref(),                   #4  
                              self.indicref,                  #5
                              self.redflagref,                #6
                              self.servref,                   #7
                              self.dexadata(),                  #8
                              self.preopreftab(),               #9
                              self.admsdata(),                  #10
                              self.tempsort_day,                #11
                              self.lookback,                    #12
                              self.rootICD_genx,                #13
                              self.gemdict,                     #14
                              self.procgem_dict,                #15
                              self.uniqueflagx,                 #16
                              self.second_datax,               #17
                              self.window_DT]                   #18
        self._arrayidx = 0
        """ self._CWnparray is for streamlined implementations over single metric. 
        The array can be updated, and then all versions can be plugged into chooseallwisely() method with single call"""
        self._CWnparray=np.array([self._arrayidx,
                                    self.names_of_args[0],
                                    self.names_of_args[1],
                                    self.names_of_args[2],
                                    self.names_of_args[3],
                                    self.names_of_args[4],
                                    self.names_of_args[5],
                                    self.names_of_args[6],
                                    self.names_of_args[7],
                                    self.names_of_args[8],
                                    self.names_of_args[9],
                                    self.names_of_args[10],
                                    self.names_of_args[11],
                                    self.names_of_args[12],
                                    self.names_of_args[13],
                                    self.names_of_args[14],
                                    self.names_of_args[15],
                                    self.names_of_args[16],
                                    self.names_of_args[17],
                                    self.names_of_args[18]])

                                
    """
    ########################################################################################
    Property setters for class  - these are for the arguments/variables that are changed within the instance(?)
    ****************************************************************************************
    """
    @property
    def metric(self):
        return self._metric

    @metric.setter
    def metric(self, value):
        if value not in MetricClass.metricsbuilt:
            raise KeyError("{} is an invalid metric for this function".format(value))
        if (value in MetricClass.metrics_needing_tempsort_day and type(self._tempsort_day)!=int):
            raise KeyError("Invalid tempsort_day value given metric")
        if self._m_counter !=0:
            raise ValueError("Unable to change class-instance's original metric type")
        else:
            self._metric = value
            self._m_counter +=1

    @property
    def tempsort_day(self):
        return self._tempsort_day

    @tempsort_day.setter
    def tempsort_day(self,value):
        self._tempsort_day=value

    @property
    def lookback(self):
        return self._lookback

    @lookback.setter
    def lookback(self,value):
        self._lookback = value

    @property
    def rootICD_genx(self):
        return self._rootICD_genx

    @rootICD_genx.setter
    def rootICD_genx(self,value):
        if (value !=9 and value !=10):
            raise KeyError("{} is an invalid ICD generation".format(value))
        self._rootICD_genx = value

    @property
    def gemdict(self):
        return self._gemdict

    @gemdict.setter
    def gemdict(self,value):
        if value not in self.pooledgems_dict.keys():
            raise KeyError("The dictionary key: '{}' does not exist.".format(value))
        self._gemdict = value 

    @property
    def procgem_dict(self):
        return self._procgem_dict
        
    @procgem_dict.setter
    def procgem_dict(self,value):
        if value not in self.pooledprocgem_dict.keys():
            raise KeyError("The dictionary key: '{}' does not exist.".format(value))
        self._procgem_dict = value 
    
    ## This was added on March 16, 2017
    @property
    def uniqueflagx(self):
        return self._uniqueflagx
        
    @uniqueflagx.setter
    def uniqueflagx(self,value):
        self._uniqueflagx = value
        
    @property
    def second_datax(self):
        return self._second_datax
    
    @second_datax.setter
    def second_datax(self,value):
        self._second_datax=value
    ###################################
    
    @property
    def indicref(self):
        return self._indicref
        
    @indicref.setter
    def indicref(self, value):
        self._indicref = value
        
    @property
    def redflagref(self):
        return self._redflagref
    
    @redflagref.setter
    def redflagref(self, value):
        self._redflagref = value
        
    @property
    def servref(self):
        return self._servref
        
    @servref.setter
    def servref(self, value):
        self._servref = value
    """
    ########################################################################################
    Class localized functions
    ****************************************************************************************
    """
    def reset_arguments(self):
        _output           =  [self.metric,                      #0
                              self.servdata(),                  #1
                              self.claimdata(),                 #2
                              self.demodata(),                  #3
                              self.demoref(),                   #4  
                              self.indicref,                  #5
                              self.redflagref,                #6
                              self.servref,                   #7
                              self.dexadata(),                  #8
                              self.preopreftab(),               #9
                              self.admsdata(),                  #10
                              self.tempsort_day,                #11
                              self.lookback,                    #12
                              self.rootICD_genx,                #13
                              self.gemdict,                     #14
                              self.procgem_dict,                #15    
                              self.uniqueflagx,                 #16
                              self.second_datax,               #17
                              self.window_DT]
        return _output

    """ Scripts that gives Class access to the chooseallwisely function """
    def run_CW(self):
#        print(self.names_of_args)
        self.names_of_args = self.reset_arguments()
        print('metric:')
        print(self.names_of_args[0])
#        print(self.names_of_args)
        self.tempsort_day_check(self.names_of_args[11], self.names_of_args[0])
        output = chooseallwisely(self.names_of_args[0],                  #metric
                        self.servdata_dict[self.names_of_args[1]],   #servdata
                        self.claimdata_dict[self.names_of_args[2]],  #claimdata
                        self.demodata_dict[self.names_of_args[3]],   #demodata
                        self.demoref_dict[self.names_of_args[4]],    #demoref
                        self.indicref_dict[self.names_of_args[5]],   #indicref
                        self.redflagref_dict[self.names_of_args[6]], #redflagref
                        self.servref_dict[self.names_of_args[7]],    #servref
                        self.dexadata_dict[self.names_of_args[8]],   #dexadata
                        self.preopreftab_dict[self.names_of_args[9]],#preopreftab
                        self.admsdata_dict[self.names_of_args[10]],  #admsdata
                                  self.names_of_args[11],       #tempsort_day
                                  self.names_of_args[12],       #lookback
                                  self.names_of_args[13],       #rootICD_genx
                        self.pooledgems_dict[self.names_of_args[14]],#gemdict  last updated 8/28
                        self.pooledprocgem_dict[self.names_of_args[15]], #procgem_dict
                                  self.names_of_args[16], #uniqueflagx
                        self.servdata_dict[self.names_of_args[17]],
                                  self.names_of_args[18])
        return output

    ## This is out-of-date
    def review_CW(self):
        raise ValueError("the review_CW method is unsupported at this time")
        self.names_of_args = self.reset_arguments()
        print('metric:')
        print(self.names_of_args[0])
#        print(self.names_of_args)
        self.tempsort_day_check(self.names_of_args[11], self.names_of_args[0])
        output = rep_CW(self.names_of_args[0],                  #metric
                        self.servdata_dict[self.names_of_args[1]],   #servdata
                        self.claimdata_dict[self.names_of_args[2]],  #claimdata
                        self.demodata_dict[self.names_of_args[3]],   #demodata
                        self.demoref_dict[self.names_of_args[4]],    #demoref
                        self.indicref_dict[self.names_of_args[5]],   #indicref
                        self.redflagref_dict[self.names_of_args[6]], #redflagref
                        self.servref_dict[self.names_of_args[7]],    #servref
                        self.dexadata_dict[self.names_of_args[8]],   #dexadata
                        self.preopreftab_dict[self.names_of_args[9]],#preopreftab
                        self.admsdata_dict[self.names_of_args[10]],  #admsdata
                                  self.names_of_args[11],       #tempsort_day
                                  self.names_of_args[12],       #lookback
                                  self.names_of_args[13],       #rootICD_genx
                        self.pooledgems_dict[self.names_of_args[14]],#gemdict  last updated 8/28
                        self.pooledprocgem_dict[self.names_of_args[15]], #procgem_dict
                                  self.names_of_args[16], #uniqueflagx
                        self.servdata_dict[self.names_of_args[17]], # second servdata
                                  self.names_of_args[18])
        return output

    def run_baseCW(self):
        """run_baseCW takes standard ICD10 -> ICD9 conversions as stored in the Research Derivative (RD)
                      and applies the original Colla-selected ICD9 criteria
           (however, as of March 16, 2017 this may not be supported)
        """
        print("This method is likely not supported - as of March 16, 2017")
        return self.run_CW()

    """ 
    ____________________________________
    Standard cases of criteria to ICD 10
    ************************************ """
    def run_refto10_gems(self):
        _temp_gemdict = self.gemdict
        _temp_procgem = self.procgem_dict
        _temp_rootICD = self.rootICD_genx
        self.gemdict        = 'gems_s'
        self.procgem_dict   = 'gems_ps'
        self.rootICD_genx   = 9
        self.names_of_args = self.reset_arguments()
        ## process
        #print(self.names_of_args)
        _output = self.run_CW()
        ## prep for completion
        self.gemdict        = _temp_gemdict
        self.procgem_dict   = _temp_procgem
        self.rootICD_genx   = _temp_rootICD 
        self.names_of_args = self.reset_arguments()
        return _output
        
    def run_refto10_bestMap9(self):
        _temp_gemdict = self.gemdict
        _temp_rootICD = self.rootICD_genx
        self.gemdict        = 'bestMap9_s'
        self.rootICD_genx   = 9
        self.names_of_args  = self.reset_arguments()
        ## process
        _output = self.run_CW()
        ## prep for completion
        self.gemdict        = _temp_gemdict
        self.rootICD_genx   = _temp_rootICD 
        self.names_of_args = self.reset_arguments()
        return _output

    """ Simple cases of claims back to ICD 9 """
    def run_claimto9_bestMap10(self):
        _temp_gemdict = self.gemdict
        _temp_rootICD = self.rootICD_genx
        self.gemdict        = 'bestMap10_s'
        self.rootICD_genx   = 10
        self.names_of_args = self.reset_arguments()
        ## process
        _output = self.run_CW()   
        ## prep for completion
        self.gemdict        = _temp_gemdict
        self.rootICD_genx   = _temp_rootICD 
        self.names_of_args = self.reset_arguments()
        return _output


    def run_claimto9_reimb(self):
        _temp_gemdict = self.gemdict
        _temp_rootICD = self.rootICD_genx
        self.gemdict        = 'reimburse_s'
        self.rootICD_genx   = 10
        self.names_of_args = self.reset_arguments()
        ## process
        _output = self.run_CW()
        ## prep for completion
        self.gemdict        = _temp_gemdict
        self.rootICD_genx   = _temp_rootICD
        self.names_of_args = self.reset_arguments() 
        return _output

    """Special case of using the new reference data, for 10 to 10 assessment. """
    def run_10to10(self):
        # setup
        _temp_gemdict = self.gemdict
        _temp_rootICD = self.rootICD_genx
        _temp_uniqueflagx = self.uniqueflagx
        _temp_indicref   = self.indicref 
        _temp_redflagref = self.redflagref
        _temp_servref    = self.servref
        self.gemdict        = 'DEFAULT'
        self.rootICD_genx   = 10
        self.uniqueflagx   = True
        self.indicref  = 'new_INDICREF'             
        self.redflagref= 'new_REDFLAGREF'            
        self.servref   = 'new_SERVREF' 
        self.names_of_args = self.reset_arguments()
        # run
        _output = self.run_CW()
        ## reset to standard upon completion
        self.gemdict        = _temp_gemdict
        self.rootICD_genx   = _temp_rootICD
        self.uniqueflagx    = _temp_uniqueflagx
        self.indicref       = _temp_indicref
        self.redflagref     = _temp_redflagref
        self.servref        = _temp_servref
        self.names_of_args = self.reset_arguments() 
        return _output

    # this is unsupported
    def review_Cto9_reimb(self):
        raise ValueError("cls.review_Cto9_reimb() is no longer supported [March 16, 2017]")
        _temp_gemdict = self.gemdict
        _temp_rootICD = self.rootICD_genx
        self.gemdict        = 'reimburse_s'
        self.rootICD_genx   = 10
        self.names_of_args = self.reset_arguments()
        ## process
        _output = self.review_CW()
        ## prep for completion
        self.gemdict        = _temp_gemdict
        self.rootICD_genx   = _temp_rootICD
        self.names_of_args = self.reset_arguments() 
        return _output

    def tempsort_day_check(self, a,b):
        if (type(a) is not int and b in self.metrics_needing_tempsort_day):
            print("conditional rule detected\n")
            raise KeyError("Invalid tempsort_day value given the specified metric")
        else:
            pass
    
    
    """
    ########################################################################################
    Callable features of the class 
   
    (These provide information about the implementations)
    ****************************************************************************************
    """                        
    def __call__(self, value):

        self.list_var = {'CW._metric':self.metric, 'CW._servdata':self.servdata(),
                         'CW._claimdata':self.claimdata(), 'CW._demodata':self.demodata(),
                         'CW._demoref':self.demoref(), 'CW._indicref':self.indicref,
                         'CW._redflagref':self.redflagref, 'CW._servref': self.servref, 
                         'CW._dexadata':self.dexadata(), 'CW._preopreftab':self.preopreftab(), 
                         'CW._admsdata':self.admsdata(), 'CW._tempsort_day':self.tempsort_day, 
                         'CW._lookback':self.lookback, 'CW._rootICD_genx': self.rootICD_genx,
                         'CW._gemdict':self.gemdict, 'CW._procgem_dict':self.procgem_dict, 
                         'CW._uniqueflagx':self.uniqueflagx, 'CW._second_datax':self.second_datax,
                         'CW._window_DT':self.window_DT}
        if value is 'help':
            print("Keyword strings for callable functionality include:\n\
                    'help'\n\
                    'parameters'\n\
                    'fullreport'\n\
                    'dictionaries'\n\n")
        
        elif value is 'parameters':
            listing = []
            for k,v in self.list_var.items():
                listing.append(("{} is set at: {}".format(k,v)))
            print("Current parameters for this metric:\
                 \n**********************************") 
            for line in sorted(listing):
                print(line)
        
        elif value is 'fullreport':
            # this block is unvetted.
            print("This report relies on all parameters to be fixed in 'self._CWnparray' prior to execution."\
                    "\nUse MetricClass.updateCWnparray() to commit current parameters to the array."\
                    "\nCurrent complete array is:")
            self.CWnparray()
            Tcurrent_array = self.transformCWnparray()
            output = map(chooseallwisely, self.Tcurrent_array[1],
                                          self.servdata_dict[Tcurrent_array[2]],
                                          self.claimdata_dict[Tcurrent_array[3]],
                                          self.demodata_dict[Tcurrent_array[4]],
                                          self.demoref_dict[Tcurrent_array[5]],
                                          self.indicref_dict[Tcurrent_array[6]],
                                          self.redflagref_dict[Tcurrent_array[7]],
                                          self.servref_dict[Tcurrent_array[8]],
                                          self.dexadata_dict[Tcurrent_array[9]],
                                          self.preoprefdata_dict[Tcurrent_array[10]],
                                          self.admsdata_dict[Tcurrent_array[11]],
                                                         Tcurrent_array[12], # tempsort_day
                                                         Tcurrent_array[13], # lookback
                                                         Tcurrent_array[14], # rootICD_genx
                                          self.pooledgems_dict[Tcurrent_array[15]], #claims gem
                                          self.pooledprocgem_dict[Tcurrent_array[16]], # procedure gem
                                                         Tcurrent_array[17],        # uniqueflagx
                                          self.servdata_dict[self.Tcurrent_array[18]],
                                                         Tcurrent_array[19])


      
        elif value is 'dictionaries':
            print("The current dictionaries maintained in this class - not at instance level - are:\n\
                            servdata_dict = {...}\n\
                            claimdata_dict = {...}\n\
                            demodata_dict = {...}\n\
                            demoref_dict = {...}\n\
                            indicref_dict = {...}\n\
                            redflagref_dict = {...}\n\
                            servref_dict = {...}\n\
                            dexadata_dict = {...}\n\
                            preopreftab_dict = {...}\n\
                            admsdata_dict = {...}\n\
                            pooledgems_dict = {...}\n\
                            procgem_dict = {...}\n\n\n\n\
                   Each dictionary's keys can be accessed by >>>any_name_of_a_class_instance.specific_dictionary.keys()")


    """
    ############################
    ? I forget what these are called, but these are for the values that remain constant or are built-upon within the instance.
    ****************************
    """
    def servdata(self):
        return self._servdata
    def claimdata(self):
        return self._claimdata
    def demodata(self):
        return self._demodata

    def demoref(self):
        return self._demoref
#    def indicref(self):
#        return self._indicref
#    def redflagref(self):
#        return self._redflagref
#    def servref(self):
#        return self._servref
    def dexadata(self):
        return self._dexadata
    def preopreftab(self):
        return self._preopreftab
    def admsdata(self):
        return self._admsdata
    def CWnparray(self):
        return self._CWnparray
    def updateCWnparray(self):
        self._arrayidx +=1
        _temp = np.array([self._arrayidx,
                         self.names_of_args[0],
                         self.names_of_args[1],
                         self.names_of_args[2],
                         self.names_of_args[3],
                         self.names_of_args[4],
                         self.names_of_args[5],
                         self.names_of_args[6],
                         self.names_of_args[7],
                         self.names_of_args[8],
                         self.names_of_args[9],
                         self.names_of_args[10],
                         self.names_of_args[11],
                         self.names_of_args[12],
                         self.names_of_args[13],
                         self.names_of_args[14],
                         self.names_of_args[15],
                         self.names_of_args[16],  #includes uniqueflagx
                         self.names_of_args[17],
                         self.names_of_args[18]])
        self._CWnparray= np.vstack([self._CWnparray, _temp])
        return self._CWnparray
    def transformCWnparray(self):
        return self._CWnparray.T


    

