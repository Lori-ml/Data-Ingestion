''' 
This script contains the main function which calls  run_streamlit_interface function from  streamlit_interface file

'''

import streamlit as st
import pandas as pd
from streamlit_interface import run_streamlit_interface


def main():
    run_streamlit_interface()



if __name__ == "__main__":
    main()


