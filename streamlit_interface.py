"""
This script defines a Streamlit web interface for data processing, analysis, and interaction with a SQLite database. 
It utilizes functions from data_processing.py for data transformations and analysis, as well as utility functions from utilities.py.

"""

import streamlit as st
import pandas as pd
import time
import sqlite3
import json
import traceback
from data_processing import apply_transformations, chatGPT_analysis, process_sql_query, quote_identifier , process_column
from utilities import to_csv_download_link
from sqlalchemy import create_engine
from base64 import b64encode


title_styles = """
<style>
.centered-title {
  color: #779ecb;
  text-align: center;
}
</style>
"""




original_df = None
trans_df = None

def run_streamlit_interface():

    """ Defines the main Streamlit interface for data processing. It includes sections for uploading datasets, applying transformations, 
          exporting data to an SQLite database, executing SQL queries, exporting data to CSV, analyzing data with ChatGPT, 
          and dropping tables from the database.
          
          Global Variables:  original_df: Global variable to store the original DataFrame.
                             trans_df: Global variable to store the transformed DataFrame. """

    global original_df
    global trans_df

    st.markdown(title_styles, unsafe_allow_html=True)

    if 'transformations_applied' not in st.session_state:
        st.session_state.transformations_applied = False

    if 'show_sections' not in st.session_state:
        st.session_state.show_sections = False


    """ 1. Data Ingestion Section: 

       Allows users to upload a CSV or Parquet file for processing.
       Displays the original DataFrame.

    """
    
    st.markdown("<h1 class='centered-title'> Data Ingestion Tool </h1>", unsafe_allow_html=True)
    st.markdown("<div style='padding: 15px;'></div>", unsafe_allow_html=True)
    st.markdown("<h5 style='text-align: left; color: black;'>1. Upload your dataset for processing</h5>", unsafe_allow_html=True)

    uploaded_file = st.file_uploader("Choose a CSV or Parquet file", type=["csv", "parquet"])

    if uploaded_file is not None:
        if uploaded_file.type == "text/csv":
            original_df = pd.read_csv(uploaded_file)
        else:
            original_df = pd.read_parquet(uploaded_file)

        st.write("Displaying the DataFrame:")
        st.write(original_df)






    """ 2. Transformation Section: 

        Allows users to upload a JSON file specifying transformations to apply.
        Displays the loaded transformation configuration.
        Applies transformations to the original DataFrame on button click.
        Displays the transformed DataFrame.
        
    """

    st.markdown("<div style='padding: 50px;'></div>", unsafe_allow_html=True)
    st.markdown("<h5 style='text-align: left; color: black;'>2. Upload the transformations you want to apply</h5>",
                unsafe_allow_html=True)
    uploaded_json = st.file_uploader("Upload a JSON file for transformations", type=["json"])

    if uploaded_json is not None:
        try:
            config = json.load(uploaded_json)
        except json.JSONDecodeError:
            st.warning("Invalid JSON file.")
            config = None

        if config:
            
            data = []
            columns = ['Transformation']

           
            for col_name in config.keys():
                if col_name not in columns:
                    columns.append(col_name)

            
            transformation_rows = {}
            for col_name, transformations in config.items():
                for transformation_type, transformation_value in transformations.items():
                    if transformation_type not in transformation_rows:
                        transformation_rows[transformation_type] = [None] * len(columns)
                        transformation_rows[transformation_type][0] = transformation_type
                    col_index = columns.index(col_name)
                    if transformation_type == "apply":
                        function_name = transformation_value["function"]
                        transformation_rows[transformation_type][col_index] = json.dumps({
                            "type": "custom",
                            "function": function_name
                        })
                    else:
                        transformation_rows[transformation_type][col_index] = json.dumps(transformation_value)

            
            for row in transformation_rows.values():
                data.append(row)

            config_table = pd.DataFrame(data, columns=columns)

            st.write("Loaded Transformation Config:")
            st.write(config_table)
        else:
            st.warning("No JSON configuration loaded.")

    if 'transformations_applied' not in st.session_state:
        st.session_state.transformations_applied = False

    if st.button("Apply Transformations"):
        if original_df is not None and uploaded_json is not None:
            with st.spinner('Applying transformations...'):
                time.sleep(2)  
                trans_df = apply_transformations(original_df, config)
                st.session_state.show_sections = True
                st.session_state.transformations_applied = True
            st.success("Transformations Applied!")
            st.write(trans_df)
        else:
            st.warning("Please upload a JSON file with the transformations you need to apply.")






    """ 3. Data Export to SQL Section:
            - Allows users to export the DataFrame to an SQLite database.
            - Options to create a new table or insert into an existing table.
            - Displays success or error messages.   """



    st.markdown("<div style='padding: 50px;'></div>", unsafe_allow_html=True)

    if st.session_state.show_sections:

        st.markdown("<h5 style='text-align: left; color: black;'>3. Data Export to SQL Database</h5>", unsafe_allow_html=True)
        sql_export_option = st.selectbox('Select export option:', ['Create table and insert data', 'Insert into already existing table'])

        
        table_name = ''
        if sql_export_option == 'Create table and insert data':
            table_name = st.text_input('Enter the table name:', value='')  

            if st.button("Export to SQL Table"):
                if trans_df is not None and table_name:
                    try:
                        with sqlite3.connect('cleaned_data.db') as conn:
                            trans_df.to_sql(table_name, conn, if_exists='replace', index=False)
                        st.success(f"Data has been exported to the SQL database with table name '{table_name}'.")
                    except Exception as e:
                        st.error(f"An error occurred: {e}")
                else:
                    st.warning(
                        "No DataFrame to export or table name not specified. Please apply transformations first and ensure the table name is entered.")

        elif sql_export_option == 'Insert into already existing table':
            
            with sqlite3.connect('cleaned_data.db') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                table_names = [table[0] for table in tables]

            
            selected_table = st.selectbox('Select table:', table_names)
            if st.button("Export to SQL Table"):
                if trans_df is not None and selected_table:
                    try:
                        with sqlite3.connect('cleaned_data.db') as conn:
                            delete_stmt = f"DELETE FROM `{selected_table}`"  
                            conn.execute(delete_stmt)
                            conn.commit()
                            trans_df.to_sql(selected_table, conn, if_exists='append', index=False)

                        st.success(
                            f"Data has been inserted into the existing SQL database with table name '{selected_table}'.")
                    except Exception as e:
                        st.error(f"An error occurred: {e}")
                else:
                    st.warning(
                        "No DataFrame to export or no table selected. Please upload and apply transformations first and select a table.")






        
        """ 4. SQL Query Section::
            - Allows users to enter and execute SQL queries on the data in the SQLite database.
            - Displays query results or error messages. """
        


        st.markdown("<div style='padding: 50px;'></div>", unsafe_allow_html=True)
        st.markdown("<h5 style='text-align: left; color: black;'>4. Query Data from SQL Database</h5>", unsafe_allow_html=True)
        
        
        sql_query = st.text_input('Enter your SQL query here:', value='')

        
        if st.button("Execute SQL Query"):
            if sql_query:  
                processed_sql_query = process_sql_query(sql_query)
                try:
                    engine = create_engine('sqlite:///cleaned_data.db')
                    query_result_df = pd.read_sql(processed_sql_query, engine)
                    if not query_result_df.empty:
                        st.write("Query Result:")
                        st.dataframe(query_result_df)
                    else:
                        st.info("The query executed successfully but returned no results.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                    
                    
                    st.text(traceback.format_exc())
            else:
                st.warning("Please enter a SQL query to execute.")








        """ 5. Data Export to CSV Section: 
               - Allows users to export the transformed DataFrame to a CSV file.
               - Displays a download link for the CSV file.
        """


        st.markdown("<div style='padding: 50px;'></div>", unsafe_allow_html=True)
        st.markdown("<h5 style='text-align: left; color: black;'>5. Data export to CSV</h5>", unsafe_allow_html=True)
        csv_file_name = st.text_input('Enter the CSV file name:', value='')
            
            
        if st.button("Export to CSV"):
            if trans_df is not None:
                
                csv_content = trans_df.to_csv(index=False)
                csv_content_b64 = b64encode(csv_content.encode()).decode()
                href = f'data:text/csv;base64,{csv_content_b64}'
                st.markdown(f'<a href="{href}" download="{csv_file_name}.csv" target="_blank">Click here to download {csv_file_name}.csv</a>', unsafe_allow_html=True)
            else:
                st.warning("No DataFrame to export. Please upload and apply transformations first.")

        




        """6. ChatGPT Analysis Section
              - Allows users to export the transformed DataFrame to a CSV file.
              - Displays a download link for the CSV file. """


        st.markdown("<div style='padding: 50px;'></div>", unsafe_allow_html=True)
        st.markdown("<h5 style='text-align: left; color: black;'>6. Describe Sample Dataset with ChatGPT API</h5>", unsafe_allow_html=True)

           
        if st.button("Analyze Data with ChatGPT"):
                if trans_df  is not None:
                    with st.spinner('Analyzing data with ChatGPT...'):
                        analysis_result = chatGPT_analysis(trans_df )
                    st.markdown("ChatGPT Analysis:", unsafe_allow_html=True)
                    st.markdown(f"<div style='background-color: #F1FFF1; padding: 10px;'>{analysis_result}</div>", unsafe_allow_html=True)
                else:
                    st.warning("No DataFrame to analyze. Please upload and apply transformations first.")
        


        """7. ChatGPT Analysis Section: 
            - Allows users to analyze the data using ChatGPT for generating column 
            - Displays ChatGPT analysis results. """
     

        st.markdown("<div style='padding: 50px;'></div>", unsafe_allow_html=True)
        st.markdown("<h5 style='text-align: left; color: black;'>7. Drop Tables </h5>", unsafe_allow_html=True)

        
        with sqlite3.connect('cleaned_data.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]




        
        """8. Drop Tables Section : 
            - Allows users to drop tables from the SQLite database.
            - Multiselects tables to drop and displays success or error messages. """
        
        tables_to_drop = st.multiselect("Select tables to drop:", table_names)
        if st.button("Drop Selected") and tables_to_drop:
            with sqlite3.connect('cleaned_data.db') as conn:
                cursor = conn.cursor()
                for table in tables_to_drop:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS `{table}`;")
                        conn.commit()
                        st.success(f"Table '{table}' has been dropped.")
                    except Exception as e:
                        st.error(f"An error occurred while dropping table '{table}': {e}")



