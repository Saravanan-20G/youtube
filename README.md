# YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit

# Introduction

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

# Key Technologies and Skills

Python
MongoDB
Postgres Sql
Streamlit

# Required Packages

pip install google-api-python-client
pip install pymongo
pip install pandas
pip install pymysql
pip install streamlit

# Process

# Set Up API Keys: 
Obtain API keys for the YouTube Data API from the Google Developer Console.

# Data Retrieval: 
Harvest YouTube channel details, video details, and comments using the YouTube API.

# Data Storage (MongoDB): 
Store the harvested data in a MongoDB database for temporary storage.

# Data Transformation: 
Transform the raw data into a suitable format for warehousing in a relational database.

# Data Warehousing (MySQL): 
Create tables in a MySQL server and upload the transformed data for long-term storage.

# Streamlit Development: 
Develop an interactive web application using Streamlit for querying and visualizing insights from the warehoused data.
# Testing and Deployment:
Test the application thoroughly and deploy it to a suitable hosting platform.


# Conclude

Finally, create a Dashboard by using Streamlit with the retrived data and give dropdown options on the Dashboard to the user for selecting a question from that menu to analyse the data and show the output in Dataframe Table
