from bs4 import BeautifulSoup
import pandas as pd
import os
from joblib import Parallel, delayed
import re
import json
from tqdm import tqdm
import glob
import os
from bs4 import BeautifulSoup
import pickle
import re
import itertools
import glob
import getpass
from _utilities_10k_extraction import return_longest, clean_text, extract_date, extract_sections, extract_MDA



base_dir = r"C:\EXTRACT"



########################################################
# DATE ###### DATE ###### DATE ###### DATE ###### DATE #
########################################################

def extract_all_dates(files, year, njobs=-1):
    outfile = f'{base_dir}/0_DATA/0_RAW/10K/CONFORMED_PERIOD_OF_REPORT_{year}.pickle'
    if os.path.exists(outfile): return
    print("Doing", year)
    res = Parallel(n_jobs=njobs)(delayed(extract_date)(filename) for filename in tqdm(files))
    res = {elem[0]: elem[1] for elem in res}
    pickle.dump(res, open(outfile, "wb"))


#######################################################################
# BUSINESS AND RISK ###### BUSINESS AND RISK ###### BUSINESS AND RISK #
#######################################################################

def extract_business_risk_section(filename):
    def try_parser(parser='html.parser'):
        try:
            text = open(filename, encoding='utf-8').read()
            soup = BeautifulSoup(text, features=parser)
            text = soup.get_text(separator=u"\n")
            business_segment, risk_segment = extract_sections(text)
            return filename, business_segment, risk_segment
        except:
            return None

    out = try_parser(parser='html.parser')
    if out:
        return out
    out = try_parser(parser='lxml')
    if out:
        return out
    return filename, 'ERROR', 'ERROR'


def extract_br_all(files, year, njobs=16):
    outfile = f'{base_dir}/0_DATA/0_RAW/10K/Business_Risk_Segments_{year}.pickle'
    if os.path.exists(outfile): return
    print(f"Doing year {year}: {len(files)} files do to")

    res = Parallel(n_jobs=njobs)(delayed(extract_business_risk_section)(filename) for filename in tqdm(files))

    res = {elem[0]: elem[1:] for elem in res}
    pickle.dump(res, open(outfile, "wb"))


##################################################################################
# Discussion Management and Analsysis ###### Discussion Management and Analsysis #
##################################################################################

def extract_mda_section(filename):
    try:
        text = open(filename, encoding='utf-8').read()
        soup = BeautifulSoup(text, features="html.parser")
        text = soup.get_text(separator=u"\n")

        MDA = extract_MDA(text)
        return filename, MDA
    except:
        return filename, 'ERROR'


def extract_mda_all(files, year):
    outfile = fr'{base_dir}/0_Data/0_RAW/10K/Management_Discussion_{year}.pickle'
    if os.path.exists(outfile): return
    print(f"Doing year {year}: {len(files)} files do to")

    res = Parallel(n_jobs=4)(delayed(extract_mda_section)(filename) for filename in tqdm(files))

    res = {elem[0]: elem[1:] for elem in res}
    pickle.dump(res, open(outfile, "wb"))


##########################################################################
# MAIN AND TEST #### MAIN AND TEST #### MAIN AND TEST #### MAIN AND TEST #
##########################################################################

def main_br():
    main_folder = r"C:\EDGAR 10-K"
    years = [f for f in os.listdir(main_folder)][::-1]
    for year in years:
        files = glob.glob(main_folder + "/" + str(year) + r'/**/*.txt', recursive=True)
        extract_br_all(files, year)


def main_mda():
    main_folder = r"C:\EDGAR 10-K"
    years = [f for f in os.listdir(main_folder)]
    done_years = [x.split('_')[-1].split('.')[0] for x in glob.glob(r'..\..\0_Data\From 10K\Management_Discussion*')]

    to_do = [year for year in years if year not in done_years]
    for year in to_do:
        files = glob.glob(main_folder + "/" + str(year) + r'/**/*.txt', recursive=True)
        extract_mda_all(files, year)


def main_dates():
    main_folder = r"C:\EDGAR 10-K"
    years = [f for f in os.listdir(main_folder)]
    for year in years[::-1]:
        files = glob.glob(main_folder + "/" + str(year) + r'/**/*.txt', recursive=True)
        extract_all_dates(files, year)



    
    
main_br()
main_dates()
    
    
