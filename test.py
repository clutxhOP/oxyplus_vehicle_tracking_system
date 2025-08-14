from extractdata import extract_all_data
from formatdata import format_everything
import pandas as pd
def extract_and_test():
    extract_all_data()
    format_everything()
    return
def load_and_test():
    exidle = pd.read_csv("data/exidlereport/history.csv")
    df = exidle[exidle['Vehicle Number'] == '33254']
extract_and_test()