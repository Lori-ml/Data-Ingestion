''' 
This script provides utility functions for working with data and text processing.

'''

import base64
import pandas as pd


def to_csv_download_link(df, filename):
    """
    Generates a link to download a Pandas DataFrame as a CSV file.

    Input: df: Pandas DataFrame to be downloaded.
           filename: Name of the CSV file 

    Output: 
           A string representing an HTML link. Clicking the link will trigger the download of the DataFrame as a CSV file.
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">Download {filename}.csv</a>'


def remove_incomplete_sentence(text):

    ''' Removes incomplete sentences from a text. 
       
        Input: text: Input text containing sentences. 
        Output: A string with incomplete sentences removed.
        
    '''
    
    sentences = text.split(". ")
    if not text.endswith('.'):
        sentences = sentences[:-1]
    return '. '.join(sentences)