# Task 4 and 5, adding atm_iv and iv_rank and signal generation to sessions data
# run after data collection

import pandas

sessions = pandas.read_csv("data/sessions.csv")
options_df = pandas.read_csv("data/options_df.csv")

session_start=sessions["session_start"].to_list()
session_type=sessions["session_type"].to_list()
iv=options_df["entry_iv"].to_list()
atm_iv=[]

for i in range(len(session_start)):
    atm_iv.append((iv[4*i]+iv[4*i+1])/2)