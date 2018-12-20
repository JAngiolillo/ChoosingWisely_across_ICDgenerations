from IPython.core.display import HTML
HTML("<style>.container { width:100% !important; }</style>")

import pyodbc, os, math
import pandas as pd
import numpy as np    
import datetime as dt
import re
from datetime import datetime, timedelta
from pprint import pprint as pp

from DB import DB
import pickle
import xlsxwriter
from sklearn import linear_model,preprocessing
from sklearn.cross_validation import train_test_split
from statsmodels.tools import categorical
jacks_verification='My local env has loaded.'

