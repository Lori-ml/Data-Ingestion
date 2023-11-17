''' 
Different functions built for processing the data. More description can be found in each of the functions.

'''
import pandas as pd
import json
import openai
import base64
from utilities import remove_incomplete_sentence


def apply_transformations(df, config):
    """ Applies specified transformations to a Pandas DataFrame based on a provided configuration argument
        The transformations include mapping values, changing data types, and applying custom functions 
        
        Input: df: Pandas DataFrame to be transformed
               config: Configuration dictionary specifying transformations for each column.

        Output: Transformed Pandas DataFrame.
        
        """
    for column, transformations in config.items():
        if column in df.columns:
           
            mapping = transformations.get('map', None)
            if mapping is not None:
                df[column] = df[column].map(mapping).fillna(df[column])

           
            astype = transformations.get('astype', None)
            if astype is not None:
                df[column] = df[column].astype(astype)


            # Handle 'apply' transformation
            apply_config = transformations.get('apply', None)
            if apply_config is not None:
                if apply_config["type"] == "custom":
                    function_name = apply_config["function"]
                    custom_function = globals().get(function_name)
                    if custom_function:
                        df[column] = df[column].apply(custom_function)
                    else:
                        st.warning(f"Warning: Custom function '{function_name}' not found.")

        else:
            st.warning(f"Warning: Column '{column}' specified in the JSON config is not present in the DataFrame.")
    return df






def chatGPT_analysis(df, sample_size=1000):
    """ 
    Performs basic analytics on a DataFrame using ChatGPT API to generate short descriptions for each column. 
    The analysis includes average for numerical columns, unique values count, and most common value for categorical columns.
    
    Input: df: Pandas DataFrame for analysis.
           sample_size: Number of rows to sample for analysis (default is 1000).

   Output: A string containing the analysis results.

    """
    
    df_sample = df.sample(n=min(sample_size, len(df)))
    analysis_result = "Basic Analytics\n\n"
    openai.api_key = ""  
    for column in df_sample.columns:
        analysis_result += f"{column}\n\n"
        if df_sample[column].dtype in ['float64', 'int64']:
            average_value = df_sample[column].mean()
            analysis_result += f"Average: {average_value:.2f}\n"
        else:
            unique_values_count = df_sample[column].nunique()
            most_common_value = df_sample[column].mode()[0]
            analysis_result += f"Unique Values: {unique_values_count}, Most Common: {most_common_value}\n"
        prompt_text = (
            f"Provide a short and simple description for the column '{column}'."
        )
        
        
        response = openai.Completion.create(
            engine="text-davinci-002",
            prompt=prompt_text,
            max_tokens=50  
        )
        
        
        column_analysis = response.choices[0].text.strip()
        column_analysis = remove_incomplete_sentence(column_analysis)
        analysis_result += f"ChatGPT: {column_analysis}\n\n"
    
    return analysis_result






def quote_identifier(identifier):
    """Quotes SQL identifiers such as table and column names if they are not already quoted. 
    
       Input: identifier: SQL identifier to be quoted.

       Output: Quoted SQL identifier
    
    """

    if not (identifier.startswith('"') and identifier.endswith('"')):
        identifier = identifier.replace('"', '')
        identifier = f'"{identifier}"'
    return identifier
    





def process_sql_query(sql_query):

    """Processes a SQL query, quoting identifiers after keywords like "FROM" and "JOIN" to ensure proper formatting. 
    
       Input: sql_query: Input SQL query.

       Output: Processed SQL query. """

    sql_parts = sql_query.split()
    processed_parts = []
    previous_word = ""
    for word in sql_parts:
        if previous_word.upper() in ["FROM", "JOIN"]:
            word = quote_identifier(word)
        processed_parts.append(word)
        previous_word = word
    return ' '.join(processed_parts)  






def remove_dollar_sign(value):
    """ Removes the dollar sign and converts the value from string to a float. 
    
        Input: value: Dollar value as a string.

        Output: Float value without the dollar sign. """

    if isinstance(value, str):
        return int(value.replace("$", ""))
    else:
        return int(value)

        



def remove_dollar_sign_float(value):
    """ Removes the dollar sign and converts the value from string to a int. 
    
        Input: value: Dollar value as a string.

        Output: Integer value without the dollar sign. """


    if isinstance(value, str):
        return float(value.replace("$", ""))
    else:
        return float(value)
    





def process_column(column_nm):
    """ Processes a column name, converting it to a string. If the column name is NaN, returns "N/A". 
        
        Input: column_nm: Column name.

        Output: Processed column name as a string.
        """
     
    if pd.isna(column_nm):
         return "N/A"
    else:
        return str(int(column_nm)) if column_nm.is_integer() else str(column_nm)
     


     

def to_csv_download_link(df, filename):
    """ Generates a link to download a Pandas DataFrame as a CSV file.
        
        Input: df: Pandas DataFrame to be downloaded
               filename: Name of the CSV file .

        Output: A string representing an HTML link. Clicking the link will trigger the download of the DataFrame as a CSV file.
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">Download {filename}.csv</a>'
