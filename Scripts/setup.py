from distutils.core import setup

setup(
    name= "Choosing_Wisely",
    version='1.2',
    py_modules= ['setup_cw_env', # local environment parameters inherited from prior programmer
        ,'preprocessgem',                        # This was module used to create GEMs that were pickled in Step_A
        'DB',                                    # module for connecting to local clinical datawarehouse
        'cw_viewsqltablecodes',                  # module including the latest SQL queries for creating database tables
        'cw_utilityfunx',                        # collection of minor function definitions
        ###############################################################################
        'CWmetric_class_preppingfor10to10',      # This is the class definition used in Step_A
        #*          The following module is the core module of the project           ##
        'chooseallwisely_preppingfor10to10',                   # This is the core module for Step_A
        ###--------------------------------------------------------------------------##
        'preprocessingclaims_preppingfor10to10',  # This module preprocesses claims with specific metric in mind, but is GEM agnostic -- Step_A uses this
        'keepindications_removeexclusions_preppingfor10to10',      # This is used by chooseallwisely_mod to select numerators by ref rules
        ###############################################################################
        # the following modules are useful for debugging, but require ability to edit python scripts as needed #
        'chooseallwisely_mod_10to10rep',         # This is the main module for Step_Dc to examine single metric to debug
        'preprocessingclaims_reportable_2017_07_11', # This is used in Step_Dc, but requires ability to edit this python file as needed
        'keepind_ex_testingreportable_20170710', # This is the keepindications_removeexclusions for Step_Dc, but requires ability to edit this python file as needed
    
    # metadata
    Principal_investigator = 'Colin Walsh, MD, MA',
    Research_assistant = 'John Angiolillo, MD, MBA',
    RA_email = 'ja2686@cumc.columbia.edu',
    description = 'Eleven choosing wisely metrics, with different GEM implementations',
    license = 'public domain'
    keywords = 'Choosing Wisely', 'General equivalence mapping', 'Performance metrics',
    
    )
    
