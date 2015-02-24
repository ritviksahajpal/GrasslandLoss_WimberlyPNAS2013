################################################################
# December 3, 2014
# grassland_loss.py
# email: ritvik@umd.edu
#################################################################

# Google Python Style Guide
# function_name, local_var_name, ClassName, method_name, ExceptionName, 
# GLOBAL_CONSTANT_NAME, global_var_name, module_name, package_name,  
# instance_var_name, function_parameter_name

from constants import *

date        = datetime.datetime.now().strftime("mon_%m_day_%d_%H_%M")#'mon_09_day_09_21_52'#
# Directories
cdl_dir     = 'C:\\Users\\ritvik\\Desktop\\MyDocuments\\PhD\\Projects\\CropIntensity\\input\\' # Contains input CDL files
base_dir    = 'C:\\Users\\ritvik\\Desktop\\MyDocuments\\PhD\\Projects\\Grassland_Loss_PNAS\\'
inp_dir     = 'C:\\Users\\ritvik\\workspace\\GrasslandLoss_WimberlyPNAS2013\\'
jager_dir   = base_dir+'Raw_Data\\'#'C:\\Users\\ritvik\\Documents\\PhD\\Projects\\EPIC\\ML\\Switchgrass_OakRidge\\'
county_dir  = base_dir+'Raw_Data\\'
gelfand_dir = base_dir+'Raw_Data\\USNC\\'
out_dir     = base_dir+'output'+os.sep+TAG+'_'+date+os.sep
shared_dir  = base_dir+'shared'+os.sep+TAG+os.sep
lcc_dir     = base_dir+'Land_Capability_Classes\\GIS_Files\\'
vec_dir     = base_dir+'Raw_Data\\'
# GIS data
nlcd_ras    = base_dir+'Raw_Data\\NLCD\\5State_NLCD_06.img'
county_shp  = county_dir+'5StateCounties.shp'                 
###############################################################################
#
#
#
#
###############################################################################
def backup_source_code(out_dir):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    try:
        shutil.copy(os.path.realpath(__file__),out_dir)
    except:
        print "WARNING: could not save source code file"
        
###############################################################################
#
#
#
#
###############################################################################
def make_dir_if_missing(d):
    if not os.path.exists(d):
        os.makedirs(d)  

###############################################################################
#
#
#
#
###############################################################################
def merge_csv_horizontal(list_csvs,replace):
    out_file = shared_dir+os.sep+'ZonalAll.csv'
    
    if arcpy.Exists(out_file) and not(replace):
        pass
    else:    
        data = [pandas.DataFrame.from_csv(f) for f in list_csvs]        
        df = pandas.DataFrame(columns=['VALUE'])
        
        for d in data:
            cols = [x for x in d.columns if x not in df.columns or x == 'VALUE']
            df = pandas.merge(df, d[cols], on='VALUE', how='outer', suffixes=['',''])
    
        df.to_csv(out_file)
    
    logger.info('\tMerge csv files horizontally')
    return out_file
        
###############################################################################
# merge_csv_files
#
# list_csv_files: List of csv files to merge together one below other
# Only header from topmost csv files is used
# fname: name of output file
###############################################################################    
def merge_csv_files(list_csv_files,fname):
    write_file = shared_dir+fname+'.csv'
     
    with open(write_file,'w+b') as append_file:
        need_headers = True
        for input_file in list_csv_files:
            with open(input_file,'rU') as read_file:
                headers = read_file.readline()
                if need_headers:
                    # Write the headers only if we need them
                    append_file.write(headers)
                    need_headers = False
                # Now write the rest of the input file.
                for line in read_file:
                    append_file.write(line)
    logger.info('\tAppended CSV files')
    return write_file

###############################################################################
#
#
#
#
###############################################################################
def dbf_to_csv(file_name):
    if file_name.endswith('.dbf'):
        logger.info("Converting %s to csv" % file_name)
        
        csv_fn = file_name[:-4]+ ".csv"
        
        with open(csv_fn,'wb') as csvfile:
            in_db = dbf.Dbf(file_name)
            out_csv = csv.writer(csvfile)
            names = []
            for field in in_db.header.fields:
                names.append(field.name)
            out_csv.writerow(names)
            for rec in in_db:
                out_csv.writerow(rec.fieldData)
            in_db.close()
    else:
        logger.info("\tFilename does not end with .dbf")

    return csv_fn

###############################################################################
#
#
#
#
###############################################################################
def filter_raster(state,ras,replace):    
    tmp_state      = shared_dir+'t_'+state+'_'+str(START_YEAR)[2:]+'_'+str(END_YEAR)[2:]
    filtered_state = shared_dir+'f_'+state+'_'+str(START_YEAR)[2:]+'_'+str(END_YEAR)[2:]
    null_clause    = "VALUE < 1"

    if arcpy.Exists(tmp_state) and not(replace):
        pass
    else:
        try:
            arcpy.gp.FocalStatistics_sa(ras,tmp_state,"Rectangle 5 5 CELL","MAJORITY","DATA")            
        except:
            logger.info(arcpy.GetMessages())
    logger.info('\t Filtering small pixels from state '+state)
    
    if arcpy.Exists(filtered_state) and not(replace):    
        pass
    else:
        try:
            out_null = SetNull(tmp_state,tmp_state,null_clause)
            out_null.save(filtered_state)
        except:
            logger.info(arcpy.GetMessages())
                
    logger.info('\t Nulling the filtered raster from '+state)
    return filtered_state

###############################################################################
# extract_luchange_ppr
#
#
#
###############################################################################
def extract_luchange_ppr(state,ras,replace):
    clip_ppr = shared_dir+PPR+'_'+state
    ppr_vec  = base_dir+'geoDB.gdb\\PPR.shp' 
    
    if arcpy.Exists(clip_ppr) and not(replace):            
        pass
    else:
        try:
            arcpy.gp.ExtractByMask_sa(ras,ppr_vec,clip_ppr)
        except:
            logger.info(arcpy.GetMessages())
    
    logger.info('\t Clipping LU change ras by PPR...'+clip_ppr)
    return clip_ppr

###############################################################################
# clip_lu_ras_by_nlcd
#
#
#
###############################################################################
def clip_lu_ras_by_nlcd(state,ras,wtld,replace):
    clip_ras = shared_dir+CLIP+'_'+state
    
    if arcpy.Exists(clip_ras) and not(replace):            
        pass
    else:
        try:
            arcpy.gp.ExtractByMask_sa(ras,wtld, clip_ras)
        except:
            logger.info(arcpy.GetMessages())
    
    logger.info('\t Clipping LU change ras by buffered wetlands...'+clip_ras)
    return clip_ras

###############################################################################
# tabulate_area_ras
#
#
#
###############################################################################
def tabulate_area_ras(state,ras,tab_field,fname,replace):
    out_dbf  = shared_dir+os.sep+state+fname+'.dbf'
    merg_ras = shared_dir+state+'_tab_area'

    if arcpy.Exists(out_dbf[:-4]+'.csv') and not(replace):            
        pass
    else:
        try:
            #TabulateArea(county_shp,'FIPS',ras,'VALUE',out_dbf)
            #dbf_to_csv(out_dbf)
            # Execute Times
            #out_times = Raster(ras) * 100.0
             
            # Save the output 
            #out_times.save(shared_dir+state+'_mult')
     
            # Execute Int
            #out_int = Int(shared_dir+state+'_mult')
             
            # Save the output 
            #out_int.save(shared_dir+state+'_int')
     
            # Execute Lookup
            out_ras = Lookup(ras,tab_field)
             
            # Save the output 
            out_ras.save(merg_ras)

            # Zonal stat 
            out_zsat     = ZonalStatisticsAsTable(county_shp,'FIPS',merg_ras,out_dbf, "DATA", "SUM")
            dbf_to_csv(out_dbf)           
        except:
            logger.info(arcpy.GetMessages())
    
    logger.info('\t Tabulating area for ...'+ras)
    return out_dbf[:-4]+'.csv'
    

###############################################################################
# create_buffer_wtld
#
#
#
###############################################################################
def create_buffer_wtld(state,ras,replace):
    poly_wtld   = shared_dir+NLCD+'_poly_'+state+'.shp'
    buff_wtld   = shared_dir+NLCD+'_buf_'+state+'.shp'    
                
    if arcpy.Exists(buff_wtld) and not(replace):            
        pass
    else:
        try:
            # Convert ras to vector
            arcpy.RasterToPolygon_conversion(ras,poly_wtld,"NO_SIMPLIFY","VALUE")
            # Buffer vector 
            arcpy.Buffer_analysis(poly_wtld,buff_wtld,"500 Meters","FULL","ROUND","NONE","")
        except:
            logger.info(arcpy.GetMessages())
    
    logger.info('\tBuffering wetlands from nlcd ...'+buff_wtld)
    return buff_wtld

###############################################################################
# extract_wetlands_nlcd
#
#
#
###############################################################################
def join_csv_to_ras(state,filtered_ras,ras2,False):
    state_csv = shared_dir+os.sep+state+'_filt_luc.csv'
    
    try:
        out_CR = shared_dir+os.sep+state+'_CR'
        arcpy.CopyRows_management(scsv,out_CR)        
        arcpy.BuildRasterAttributeTable_management(filtered_ras, "Overwrite")
        arcpy.JoinField_management(filtered_ras,"VALUE",ras2,"VALUE","")
        
        fields = ['VALUE','COUNT']
        with arcpy.da.SearchCursor(filtered_ras,fields) as cursor:  
            for row in cursor:  
                with open(state_csv, 'wb') as csv_file:
                    csvw = csv.writer(csv_file, delimiter=',')
                    csvw.writerow(['State','To_Crop'])
                    csvw.writerow([state,row[1]])        
    except:
        logger.info(arcpy.GetMessages())

    logger.info('\tJoin csv to raster ...'+scsv)
    return filtered_ras,state_csv

###############################################################################
# extract_wetlands_nlcd
#
#
#
###############################################################################
def extract_wetlands_nlcd(state,replace):
    state_vec   = vec_dir+state.upper()+'.shp'
    tmp_ras     = shared_dir+'tmp_'+state  
    nlcd_tmp    = shared_dir+NLCD+'_tmp_'+state
    nlcd_wtld   = shared_dir+NLCD+'_'+state    
    nlcd_recl    = shared_dir+NLCD+'_recl_'+state
    wtld_clause = "\"Value\" = 90 OR \"Value\" = 95"
                
    # Extract NLCD for state
    if arcpy.Exists(tmp_ras) and not(replace):            
        pass
    else:
        try:
            arcpy.gp.ExtractByMask_sa(nlcd_ras,state_vec,tmp_ras)
        except:
            logger.info(arcpy.GetMessages())
    logger.info('\tExtracted for state ...'+state_vec)                
            
    # Extract only the wetlands
    if arcpy.Exists(nlcd_wtld) and not(replace):            
        pass
    else:
        try:
            # Execute ExtractByAttributes
            nlcd_ext = ExtractByAttributes(tmp_ras, wtld_clause) 
            nlcd_ext.save(nlcd_tmp)
        except:
            logger.info(arcpy.GetMessages())
    logger.info('\tExtracted wetlands from nlcd ...'+tmp_ras)
    
    # Reclassify wetlands into 1 group
    if arcpy.Exists(nlcd_wtld) and not(replace):            
        pass
    else:
        try:    
            out_reclass = ReclassByASCIIFile(nlcd_tmp,NLCD_REMAP,"NODATA")        
            out_reclass.save(nlcd_recl)
        except:
            logger.info(arcpy.GetMessages())
    
    logger.info('\tReclassed wetlands from nlcd ...'+nlcd_recl)
    
    # Extract wetlands in PPR
    ppr_vec  = base_dir+'geoDB.gdb\\PPR.shp' 
    
    if arcpy.Exists(nlcd_wtld) and not(replace):            
        pass
    else:
        try:
            arcpy.gp.ExtractByMask_sa(nlcd_recl,ppr_vec,nlcd_wtld)
        except:
            logger.info(arcpy.GetMessages())
    
    logger.info('\t Clipping LU change ras by PPR...'+nlcd_wtld)
            
    return nlcd_wtld

###############################################################################
# reclassify_and_combine
#
#
#
###############################################################################
def reclassify_and_combine(state,state_cdl_files,replace):
    tmp_comb    = shared_dir+os.sep+'t_c_'+state+'_'+str(range_of_yrs[0])[2:]+'_'+str(range_of_yrs[len(range_of_yrs)-1])[2:]
    comb_raster = shared_dir+os.sep+'comb_'+state+'_'+str(range_of_yrs[0])[2:]+'_'+str(range_of_yrs[len(range_of_yrs)-1])[2:]
    null_raster = shared_dir+os.sep+'null_'+state+'_'+str(range_of_yrs[0])[2:]+'_'+str(range_of_yrs[len(range_of_yrs)-1])[2:]
    state_csv   = shared_dir+os.sep+state+'_UnFiltered_LUChange.csv'    
    to_comb_rasters = []
    
    # Create output directory for each state
    state_dir  = out_dir+os.sep+state+os.sep
    make_dir_if_missing(state_dir)
    
    # Reclassify and then extract to get corn/soy and grassland separately
    for j in range(len(range_of_yrs)):
        recl_raster = shared_dir+RECL+'_'+state+'_'+str(range_of_yrs[j])

        # Reclassify raster
        if arcpy.Exists(recl_raster) and not(replace):            
            pass
        else:
            try:
                out_reclass = ReclassByASCIIFile(state_cdl_files[j],REMAP_FILE,"NODATA")        
                out_reclass.save(recl_raster)                                                              
            except:
                logger.info(arcpy.GetMessages())
                
        to_comb_rasters.append(recl_raster)       
                                
        logger.info('\tReclassified ...'+recl_raster)   
    ### End For loop     
    
    # Combine rasters
    if arcpy.Exists(tmp_comb) and not(replace):
        pass
    else:     
        try:   
            out_combine = Combine(to_comb_rasters)
            out_combine.save(tmp_comb)
        except:
            logger.info(arcpy.GetMessages())
    logger.info('\tCombined...'+tmp_comb)
               
    # Iterate through raster and output to csv those rows
    # with LU change to/from grasslands/crops
    if arcpy.Exists(state_csv) and not (replace):
        pass
    else:
        fields    = ['RECL_'+state.upper()+"_"+str(START_YEAR),'RECL_'+state.upper()+"_"+str(END_YEAR),'COUNT']
        to_crop   = 0.0
        from_crop = 0.0
        with arcpy.da.SearchCursor(tmp_comb,fields) as cursor:  
            for row in cursor:  
                # If row[0] <> row[1], then LU change has happened
                if(row[0] <> row[1]):
                    if(row[0]==CROP):
                        from_crop = row[2]
                    else:
                        to_crop = row[2]
                else:
                    continue
        with open(state_csv, 'wb') as csv_file:
            csvw = csv.writer(csv_file, delimiter=',')
            csvw.writerow(['State','To_Crop','From_Crop'])
            csvw.writerow([state,to_crop,from_crop])
    
    if arcpy.Exists(null_raster) and not(replace):
        pass
    else:     
        try:     
            where_clause = "(\"RECL_"+state.upper()+"_"+str(START_YEAR)+"\" = "+str(CROP)+" AND \"RECL_"+\
                            state.upper()+"_"+str(END_YEAR)+"\" = "+str(CROP)+") OR ( \"RECL_"+state.upper()+\
                            "_"+str(START_YEAR)+"\" = "+str(OPEN)+" AND \"RECL_"+state.upper()+"_"+str(END_YEAR)+\
                            "\" = "+str(OPEN)+") OR ( \"RECL_"+state.upper()+"_"+str(START_YEAR)+"\" = "+str(OPEN)+\
                            " AND \"RECL_"+state.upper()+"_"+str(END_YEAR)+"\" = "+str(CROP)+")"                
            arcpy.gp.Con_sa(tmp_comb,"0",null_raster,tmp_comb,where_clause)            
        except:
            logger.info(arcpy.GetMessages())
            
    logger.info('\tNulled...'+null_raster)
            
    # Replace all NODATA values with 0
    if arcpy.Exists(comb_raster) and not(replace):
        pass
    else:     
        try:   
            out_con = Con(IsNull(null_raster),0,null_raster)
            out_con.save(comb_raster)
        except:
            logger.info(arcpy.GetMessages())
    logger.info('\tCombined (Nulled NODATA)...'+comb_raster)
    
    
    return comb_raster, state_csv

###############################################################################
#
#
#
#
###############################################################################
def compute_avg_epic_var(col_name,replace):
    ras_gelfand = gelfand_dir+'usnc'
    out_CR      = shared_dir+os.sep+col_name+'_CR'
    
    # Read in data from Gelfand et al., 2013
    gelfand_df            = pandas.read_csv(gelfand_dir+'All.csv')    
    gelfand_df[col_name] *= RAS_MULT
    gelfand_df[col_name]  = gelfand_df[col_name].astype(int)
    gelfand_df            = gelfand_df[['VALUE',col_name]]
    gelfand_df.to_csv(shared_dir+os.sep+col_name+'.csv')

    # Join Gelfand et al. 2013 data to raster    
    try:
        arcpy.CopyRows_management(shared_dir+os.sep+col_name+'.csv',out_CR)      
        arcpy.BuildRasterAttributeTable_management(ras_gelfand,"Overwrite")
        logger.info('\tCopy rows and build raster '+ras_gelfand)

        arcpy.JoinField_management(ras_gelfand,"VALUE",out_CR,"VALUE","")
        logger.info('\tJoined raster to Gelfand et al data '+ras_gelfand)
    except:
        logger.info(arcpy.GetMessages())    
    
    # Lookup
    out_name = shared_dir+os.sep+'r_'+col_name                
    if arcpy.Exists(out_name) and not(replace):
        pass
    else:        
        try:
            out_ras  = Lookup(ras_gelfand,col_name)
            out_ras.save(out_name)
        except:
            logger.info(arcpy.GetMessages())
    logger.info('\tLookup '+out_name)
    
    # Zonal statistics
    dbf_file = shared_dir+os.sep+'dbf_'+col_name
    if arcpy.Exists(dbf_file) and not(replace):
        pass
    else:
        try:
            out_zsat = ZonalStatistics(county_shp,'FIPS',out_name,"MEAN", "DATA")
            out_zsat.save(dbf_file)
            csv_fn   = dbf_to_csv(dbf_file)
        except:
            logger.info(arcpy.GetMessages())        
    logger.info('\tZonal stat ' + dbf_file)

    return csv_fn
    
###############################################################################
# main
#
#
#
###############################################################################   
if __name__ == "__main__":   
    # make output dir
    make_dir_if_missing(out_dir)
    make_dir_if_missing(shared_dir)
    # Read in all state names
    lines = open(inp_dir+os.sep+list_states, 'rb').readlines()
    
    range_of_yrs = [START_YEAR,END_YEAR]      
    
    for subdir, dir_list, files in os.walk(cdl_dir):
            break        
    
    # Logger
    LOG_FILENAME   = out_dir+os.sep+'Log_'+TAG+'_'+date+'.txt'
    logging.basicConfig(filename = LOG_FILENAME, level=logging.DEBUG,\
                        format='%(asctime)s    %(levelname)s %(module)s - %(funcName)s: %(message)s',\
                        datefmt="%Y-%m-%d %H:%M:%S") # Logging levels are DEBUG, INFO, WARNING, ERROR, and CRITICAL
    logger = logging

    # Backup source code
    backup_source_code(out_dir)
               
    # N0_WD    N0_WS    N0_NS    N0_NMN    N0_WUEF    N0_NUF    N0_YLDF    N68_YLDF    N123_YLDF    
    # N68_WS    N123_WS    N68_NS    N123_NS    N68_NUF    N123_NUF    N68_WUEF    N123_WUEF    
    # N0_DWOC    N68_DWOC    N123_DWOC    N0_Q    N68_Q    N123_Q    N0_DN    N68_DN    N123_DN    
    # N0_AVOL    N68_AVOL    N123_AVOL
    
    # For each variable of interest:
    # 1. Multiply by large number to make it integer
    # 2. Join data to raster
    # 3. Perform Lookup
    # 4. Perform Zonal Statistics (Mean) and output to table
    # 5. Read table and divide my multiplier used in step 1
    list_csv_zonal = []        
    list_epic_vars = ['N0_YLDF','N68_YLDF','N123_YLDF','N0_DWOC',\
                      'N68_DWOC','N123_DWOC','N0_DN','N68_DN','N123_DN']
    for var in list_epic_vars:
        logger.info('\tAveraging EPIC var: '+var)
        out_csv = compute_avg_epic_var(var,False)
        list_csv_zonal.append(out_csv)    

    all_csv = merge_csv_horizontal(list_csv_zonal,False) 
            
    #tab_gelfand_csv = tabulate_area_ras('WCB',ras_gelfand,'YIELD_N_0','_gelfand_tabulate',True)
    
    pdb.set_trace()               
    luchange_csvs   = []
    f_luc_csvs      = []
    tab_clip_csv    = []
    tab_jager_csv   = []
    tab_gelfand_csv = []
    # Loop across all states
    for line in lines:
        state_cdl_files = []
        # Find out state name    
        state = line.split()[0]
        print state
        logger.info(state)
        
        # Collect all CDL files for state within given year range     
        for j in range(len(range_of_yrs)):
            for position, item in enumerate(dir_list):
                if (str(range_of_yrs[j]) in item):
                    cdl_file = glob.glob(cdl_dir+os.sep+dir_list[position]+os.sep+state+os.sep+'*_'+state+'_*'+str(range_of_yrs[j])+'*.tif')
                    if cdl_file:                    
                        state_cdl_files.append(''.join(cdl_file))
                    else:
                        logger.info(cdl_file + 'not found!')
                        sys.exit(0)
        
        # Set snap extent
        if(SET_SNAP):
            arcpy.env.snapRaster     = state_cdl_files[0]
            SET_SNAP                 = False
            logger.info('\tSet snap extent')
            
        # 1. Combine CDL (START_YEAR ... END_YEAR)    
        comb_raster, scsv = reclassify_and_combine(state,state_cdl_files,False)
        luchange_csvs.append(scsv)
        
        # 2. Run majority filter
        filtered_ras      = filter_raster(state,comb_raster,False)
        
        # 3. Join filtered raster with csv containing LU change info
        filtered_ras,fcsv = join_csv_to_ras(state,filtered_ras,comb_raster,False)
        f_luc_csvs.append(fcsv)
            
        # 4. Extract wetlands from NLCD and create user-specified buffer
        wtld_nlcd         = extract_wetlands_nlcd(state,False)
                
        # 5. Create buffer around wetlands
        buff_wtld         = create_buffer_wtld(state,wtld_nlcd,False)
        
        # 6. Extract LU change in PPR
        #ppr_ras       = extract_luchange_ppr(state,filtered_ras,True)
        
        # 7. Clip LU change raster by buffered NLCD wetlands
        clip_lu_ras       = clip_lu_ras_by_nlcd(state,filtered_ras,buff_wtld,False)
                
        # 8. Tabulate area within FIPS zones
        tab_clip_csv.append(tabulate_area_ras(state,clip_lu_ras,'VALUE','_clip_tabulate',False))

        # 9. Tabulate area for Jager 
        #ras_jager         = jager_dir+'Jager_Maxylds'
        #dsc = arcpy.Describe(clip_lu_ras)
        #coord_sys = dsc.spatialReference
        #arcpy.DefineProjection_management(ras_jager,coord_sys)
                
        #tab_jager_csv.append(tabulate_area_ras(state,ras_jager,'_jager_tabulate',False))
   
    # 10. Tabulate area for Gelfand et al.
    ras_gelfand      = gelfand_dir+'usnc'
    out_CR           = shared_dir+os.sep+'Ylds_CR'
    try:
        arcpy.CopyRows_management(gelfand_dir+'Ylds.csv',out_CR)        
        arcpy.BuildRasterAttributeTable_management(ras_gelfand, "Overwrite")
        arcpy.JoinField_management(ras_gelfand,"VALUE",out_CR,"VALUE","")
    except:
        print arcpy.GetMessages()        
    tab_gelfand_csv = tabulate_area_ras('WCB',ras_gelfand,'YIELD_N_0','_gelfand_tabulate',True)
        
    # Output to-from land-use change information into csv
    merge_csv_files(luchange_csvs,'WCB_UnFilt_LUC')
    merge_csv_files(f_luc_csvs,'WCB_Filt_LUC')   
    merge_csv_files(tab_clip_csv,'WCB_Tabulate_LUC')