# Google Python Style Guide
# function_name, local_var_name, ClassName, method_name, ExceptionName, 
# GLOBAL_CONSTANT_NAME, global_var_name, module_name, package_name,  
# instance_var_name, function_parameter_name

from itertools import groupby, combinations, product
from matplotlib.ticker import FuncFormatter
from scipy import stats
from matplotlib import rcParams
import os,time,pdb,operator,csv,glob,logging,shutil,\
       datetime,numpy,sys,pandas,math,itertools,ast,brewer2mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.cm as cm
import numpy as np

# USER defined constants
base_dir = 'C:\\Users\\ritvik\\Desktop\\MyDocuments\\PhD\\Projects\\Grassland_Loss_PNAS\\shared\\GLoss\\'
RAS_AREA = 0.3136
M2_TO_HA = 10000

def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average  = numpy.average(values, weights=weights)
    variance = numpy.average((values-average)**2, weights=weights)  # Fast and numerically precise
    error    = math.sqrt(variance)/math.sqrt(len(values))

    return (average,math.sqrt(variance),error)

def roundup(x):
    return int(math.ceil(x / 100.0)) * 100

def make_dir_if_missing(d):
    if not os.path.exists(d):
        os.makedirs(d)
        
def simple_axis(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

def func(x, pos):  # formatter function takes tick label and tick position
   s = '{:0,d}'.format(int(x))
   return s

def simple_legend():
    leg = plt.legend(loc='upper left',fancybox=None)
    leg.get_frame().set_linewidth(0.0)
    leg.get_frame().set_alpha(0.5)

def format_axis(ax):
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format)

def genValues(i,j):
  return [x for x in range(j+1) if x >=i]

def rsquared(x, y):
    """ Return R^2 where x and y are array-like."""

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    return r_value**2

# COLORS
bmap = brewer2mpl.get_map('Set2','qualitative',3,reverse=True)
colors = bmap.mpl_colors

# rcParams dict
rcParams['mathtext.default'] ='regular'
rcParams['axes.labelsize']   = 11
rcParams['xtick.labelsize']  = 11
rcParams['ytick.labelsize']  = 11
rcParams['legend.fontsize']  = 11
rcParams['font.family']      = 'sans-serif'
rcParams['font.serif']       = ['Helvetica']
rcParams['figure.figsize']   = 7.3, 4.2

def plot_gelfand_results(colmns,flname,title,ylab):
    wavg_var = []
    std_var  = []
    err_var  = []
    
    lab  = ['0','33','68']
    fl   = base_dir+os.sep+flname
    df   = pandas.DataFrame.from_csv(fl,index_col=False)
    
    # Set AREA column to correct units
    df['AREA'] /= M2_TO_HA
    
    # Discard all rows with any missing values
    df.dropna(inplace=True)
    df.to_csv(base_dir+os.sep+'clean.csv')
    
    for col in colmns:
        wavg,wstd,werr = weighted_avg_and_std(df[col],df['AREA'])
        wavg_var.append(wavg)
        std_var.append(wstd)
        err_var.append(werr)
    
    fig, ax = plt.subplots(figsize=(10,6))
    ax.bar(numpy.arange(0,len(colmns))+0.5,wavg_var,width=0.4,color='grey',\
            yerr=err_var,ecolor='k',edgecolor='grey',label='+N')
    
    ax.set_xticks(numpy.arange(0,len(colmns))+0.7)
    ax.set_xticklabels(lab)
    ax.set_xlabel('Nitrogen Fertilization Level\n(kg N/ha/yr)',multialignment='center')
    ax.set_ylabel(ylab,multialignment='center')
    plt.tick_params(\
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        top='off',         # ticks along the top edge are off
        bottom='off',
        ) # labels along the bottom edge are off
    plt.tick_params(\
        axis='y',          # changes apply to the y-axis
        which='both',      # both major and minor ticks are affected
        right='off',       # ticks along the top edge are off
        ) # labels along the bottom edge are off
    spines_to_remove = ['top','right']
    for spine in spines_to_remove:
        ax.spines[spine].set_visible(False)  
    ax.set_xlim(0.0, len(colmns)+1)
    #ax.set(aspect=0.6)
    leg = ax.legend()
    leg.draw_frame(False)

    
    #ax     = sub_df.boxplot(grid=False)    

    ###### Formatting ###################################################
    #simple_axis(ax) # Simple axis, no axis on top and right of plot
    #format_axis(ax)   # Y axis numbers have ','s
    #simple_legend() # Remove box around legend    
    
    plt.tight_layout()
    plt.savefig(base_dir+title+'.png',bbox_inches='tight', dpi=600)
    
if __name__ == '__main__':
    gelfand_results = 'MergedAll.csv'

    # Plot yields
    yld_ylab        = 'EPIC Simulated Yield\n(Mg/ha)'
    yld_title       = 'yields'
    yld_colmns      = ['N0_YLDF','N68_YLDF','N123_YLDF']
    plot_gelfand_results(yld_colmns,gelfand_results,yld_title,yld_ylab)

    # Plot SOC
    woc_ylab        = 'EPIC Simulated SOC\n(Mg/ha)'
    woc_title       = 'soc'
    woc_colmns      = ['N0_DWOC','N68_DWOC','N123_DWOC']
    plot_gelfand_results(woc_colmns,gelfand_results,woc_title,woc_ylab)
