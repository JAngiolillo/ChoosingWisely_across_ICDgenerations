This project implements 11 Choosing Wisely metrics, which were defined in ICD-9, and applies them on ICD-10 based patient data.

Questions: ja2686@cumc.columbia.edu or colin.walsh@vumc.org
**************************************************************
**************************************************************



The directories here:
(1) Algorithm_implementation
(2) Scripts
(3) ref
(4) pickle

(1) Algorithm_implementation
This directory contains the jupyter notebooks necessary to run the analysis.

(2) Scripts
This directory contains the custom functions invoked by the above algorithm scripts. 

(3) ref
This directory contains the reference tables.

(4) pickle
This directory contains the general equivalence mappings.

^^Of note, the contents of Algorithm_implementation and Scripts should be in the same directory at run time. This shared directory should be parent directory of (3) ref and (4) subdirectories.




**************************************************************
The step-wise algorithms run from Step_A through Step_D.
. Step_A and Step_B generate the metric results, in absolute counts and estimated dollars spent.
. Step_Cx's and Step_D's are used for annotations and debugging.
  ---
. The python modules and the jupyter notebooks should be in same directory when implemented.
. This directory should contain the pickle and ref directories.
***************************************************************

Requirements:
. Reference data stored in the reference directory
. Live access to SQL database with patient data
. pickled files in pickle directory (GEMs)
. annotations in annotation directory

Packages needed:
. package dependencies can be found in the setup_cw_env.py file

The bulk of the logic is performed in Step_A. The setup.py file includes the critical modules needed for Step_A to run.

Overall Flow of Step_A:
(1) Load and preprocess reference data
(2) Load and preprocess patient data
(3) Create the metric class for each of the 11 metrics
(4) Run the calculations for the 11 metrics, across each of the implemented GEMs (or novel ICD-10 reference set, which obviates mapping)
(5) Export labels, cumulative and monthly reports

More detail about Step_A's #(4):
. using 19 arguments fed from the class:
(1) Check data preprocessing quality assumptions
(2) Subset PHI by metric's demographic rules
(3) Map either reference codes or patient data to the other ICD generation 
      . (For Novel ICD-10 algorithm, no mapping is done)
      . all active mappings in GEM were allowed, allowing for expansion from 1 to many
      . If mapping reference codes, map the reference codes by individual subset (redflags v. indications, etc)
      . If mapping patient data, map the claims data
(4) Start restricting patient data based on most-basic denominator rules
(5) For certain metrics, prepare multiple data streams (claims, procedures, drugs), and go from long-to-wide when necessary
      . Standardize column headings to allow convergent processing
      . Implement basic time window logic between claims and tests (low back pain, GU imagin)
      . Dump irrelevant CPTs (those neither in redflag/indication tables)
      . Merge together data streams when needed (ex: Dexa-scan measure and prior rocedure date)
      . [preprocessingclaims] module implemented
(6) Restrict the data to the study period per metric definition (overlapping window backward -lbp, v. forward -surgery)
(7) Extract the denominator for each metric
(8) From the denominator, begin to select the numerator
      . The numerator winds up being cases without justified exception to general Choosing Wisely principle
      . Red flags remove cases from numerator (ex Cauda Equina syndrome in low back pain) 
      . Indications keep cases in numerator because they are reveal inappropriate reasoning
      . Use simple time logic again where needed     .
      . [keepindications_removeexclusions] module implemented
(9) Given mappings' possible 1 - to - many, cull duplicates, and return numerator and denominator cases, at the individual patient and service level.

Overall summary of Step_B:
(1) Load output of Step_A and load reference cost tables
(2) Merge them, and sum derived costs for numerators and denominators

Overall summary of Step_C and Step_D:
+ These were used for Accuracy analysis, using Step_A's output and manual annotations
+ These were automated as full batch, and have contexts set up for individual case inspection

Overall summary of Step_x:
(1) Load output of Step_A, and randomly extract cases from numerator and denominator to send to annotators, done with single blinding.

*******************************************************
*******************************************************
More about specific modules
# preprocessingclaims() module: acts on single metric for single GEM:
     . Standardizes column headings for convergent processing
     . Merge in multiple procedure and drug streams
     . Merge these with the claims diagnosis codes
     . Apply admission time logic, consider forward/reverse window logic, and restrict to only most proximate event

# keepindications_removeexclusions() module:
     . Among relevant metrics, from the remaining cases in the numerator, remove those with valid reason for questioned service (ex cauda equina with low back pain), keep the others
     . Among metrics with specific criteria that prove low-value care, ensure those remain in the numerator.

# chooseallwisely module:
           Required Args:
                0   metric = name of the CW-metric (['lbp', 'bph', 'card', 'cerv', 'dexa', 'catpreop', 'nonpreop',    'vitd', 'psyc', 'feed', 'narc'])
                1   servdata   = unprocessed services-rendered pd.dataframe (these are procedure / drug codes)
                2   claimdata  = unprocessed ICD claims pd.dataframe 
                3   demodata   = downloadedSQL demographics from netezza
                4   demoref    = demographic rules for metric
                5   indicref   = ref lists of invalid indications given for services (??)
                6   redflagref = ref list of valid indications for the questioned services (ex cauda equina syndrome, low back pain)
                7   servref    = key for the ref list of services being examined by metric

           Optional Keyword Args:
                8   dexadata    = None (alternatively: a key for the cls.dexadata_dict)
                9   preopreftab = None (alternatively: a key for the cls.preopreftab_dict)
                10  admsdata    = None (alternatively: a key for the cls.admsdata_dict)
                11  tempsort_day= True (alternatively: False or integer(days for window of eval))
                12  lookback    = 365 (alternatively: integer(days for retrospective period))
                13  rootICD_gx  = 10 (alternatively: 9) (generation mapping from)
                14  sourcegemdict= None (needs specification)
                15  procgem_dict = None (for mapping ICD procedure codes, not actually used b/c patient data ommitted these)
                16  uniqueflag_x = None (True/False, used to switch from mapping to Novel ICD 10 to 10)
                17  second_data  = None (alternatively: brings in additional service data (drugs or surgeries for metrics that use both))
                18  window_date  = None (date for start of study period)
            
            
Main citation: 
This work is an adaptation of a study by Colla et al that was published in JGIM in 2014:
https://www.ncbi.nlm.nih.gov/pubmed/25373832
PMID: 25373832 
