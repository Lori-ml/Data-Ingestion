# Project Description

This tool provides a streamlined solution for users to perform various data operations within a single interface. Key functionalities include:

**1.File Insertion:** Easily upload CSV or Parquet files, up to 1 GB in size.

**2.Transformation:** Apply transformations using a JSON file to customize and manipulate data according to user's requirements.

**3.SQLite Database Integration:** Save transformed data directly to a SQLite database.

**4.Query Execution:** Seamlessly execute queries on the stored data, enhancing flexibility and data exploration.

**5.Export to CSV:** Save query results as .CSV files, facilitating data sharing and further analysis.

**6.ChatGPT Integration:** Analyze data interactively through ChatGPT, gain valuable insights and enhance the overall analytical experience.

## File Description


**data_processing.py:** This file contains functions needed to build and process the data.

**streamlit_interface.py:** This script defines a Streamlit web interface for data processing, analysis, and interaction with a SQLite database. 
It utilizes functions from data_processing.py for data transformations and analysis, as well as utility functions from utilities.py.

**utilities.py:** Provides utility functions for working with data and text processing.

**main.py:** contains the main function which calls  *run_streamlit_interface* function from  *streamlit_interface file*. Use following commands to run the application :


```bash
streamlit run main.py
```

## Results:

https://github.com/Lori-ml/Data-Ingestion-App/assets/41455899/88caaeaa-ca27-4637-8be7-0d102103b70c

