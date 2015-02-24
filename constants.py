import os, time, pdb, operator, csv, glob, logging, shutil, arcpy, datetime, numpy, sys, pandas
from dbfpy import dbf
from arcpy.sa import *
from itertools import groupby, combinations, combinations_with_replacement
from collections import Counter

# Arcpy constants
arcpy.CheckOutExtension("spatial")
arcpy.env.overwriteOutput= True
arcpy.env.extent         = "MAXOF"

# USER MODIFIED PARAMETERS #####################################################
START_YEAR  = 2006      # Starting year for analysis
END_YEAR    = 2011      # Ending year for analysis
TAG         = 'GLoss'   # Tag for output folder
list_states = 'WCB.txt' #'states_48.txt'
SET_SNAP    = True      # Whether to snap raster to original extent or not
FILTER_SIZE = 25        # Minimum mapping unit pixel size
RAS_MULT    = 1000      # Multiplier to convert raster data to integer
################################################################################

# CONSTANTS
M2_TO_HA    = 0.0001
REMAP_FILE  = 'recl.txt'
NLCD_REMAP  = 'nlcd_recl.txt'
CONVERSION  = 'conversion'
RECL        = 'recl'
EXTR        = 'extr'
NLCD        = 'nlcd'
CLIP        = 'clip'
PPR         = 'ppr'
CROP        = 499
OPEN        = 500
