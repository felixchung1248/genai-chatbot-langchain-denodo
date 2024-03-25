#----------------------------------
# IMPORTS
#----------------------------------
import sqlalchemy as db
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import requests
from flask import Flask,request

from langchain.agents import create_spark_sql_agent
from langchain_community.agent_toolkits import SparkSQLToolkit
from langchain_community.utilities.spark_sql import SparkSQL
from langchain_openai import ChatOpenAI
from langchain_core import exceptions
import re
import os

#----------------------------------
# Setup
#----------------------------------
app = Flask(__name__)

def convert_to_quoted_path(path):
    # Split the path by slashes and quote each part
    parts = path.split('/')
    quoted_parts = ['"{}"'.format(part) for part in parts]
    # Join the quoted parts with periods
    quoted_path = '.'.join(quoted_parts)
    return quoted_path

# def make_query(query, client, headers):
#     ## Get Schema Description and build headers
#     flight_desc = flight.FlightDescriptor.for_command(query)
#     options = flight.FlightCallOptions(headers=headers)
#     schema = client.get_schema(flight_desc, options)

#     ## Get ticket to for query execution, used to get results
#     flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
    
#     ## Get Results 
#     results = client.do_get(flight_info.endpoints[0].ticket, options)
#     return results


# Initialize Spark session
spark = SparkSession.builder \
    .appName("DenodoToSparkSQLExample") \
    .config('spark.network.timeout', '800s') \
    .config('spark.executor.heartbeatInterval', '120s') \
    .getOrCreate()
schema = "langchain_example"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
spark.sql(f"USE {schema}")      
    

engine=db.create_engine("denodo://admin:admin@datamgmtdemo01.eastasia.cloudapp.azure.com:30996/admin")
view_list=engine.execute("LIST VIEWS ALL")

# Iterate over the result set and print each row
for row in view_list:
    view_name = row[0]
    with engine.connect() as connection:
        # Pandas DataFrame
        result_proxy = engine.execute(f"select * from {view_name}")
        pandas_df = pd.DataFrame(result_proxy.fetchall())
        pandas_df.columns = result_proxy.keys()
        # Convert the Pandas DataFrame to a Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)

        # Write the Spark DataFrame to a Spark table
        spark_df.write.saveAsTable(view_name)
    


# Close the result set to free resources
view_list.close()

    # result_array = []

#     for path in data:
#         lastField = path.split("/")[-1]
#         path = convert_to_quoted_path(path)
#         #----------------------------------
#         # Run Query
#         #----------------------------------

#         ## Query Dremio, get back Arrow FlightStreamReader
#         print(f"Making query for {path}")
#         results = make_query(
#         f"""
#         SELECT * FROM {path}; 
#         """
#         , client, headers)

#         print(f"Fetching result for {path}")
#         ## Convert StreamReader into an Arrow Table
#         table = results.read_all()
#         sdf = spark.createDataFrame(table.to_pandas())
#         # Create a temporary view to run Spark SQL queries
#         sdf.write.saveAsTable(lastField)

# else:
#     print(f"Failed to fetch data: {response.status_code} {response.reason}")
    
spark_sql = SparkSQL(schema=schema)
llm = ChatOpenAI(model="gpt-4-0125-preview",temperature=0)
toolkit = SparkSQLToolkit(db=spark_sql, llm=llm)
agent_executor = create_spark_sql_agent(llm=llm, toolkit=toolkit, verbose=True,handle_parsing_errors=True) 

@app.route('/genai-response', methods=['POST'])
def genAiResponse():
    # Get the JSON from the POST request body
    try:       
        json_array = request.get_json()
        msg = json_array.get('msg')       
        result = agent_executor.run(msg)
        return result
    except exceptions.OutputParserException as e:
        # Handle the specific OutputParserException
        error_message = str(e)
        print(f"OutputParserException caught: {error_message}", flush=True)
        # Extract meaningful error message if it matches the expected pattern
        if error_message.startswith("Could not parse LLM output: `"):
            error_message = error_message.removeprefix("Could not parse LLM output: `").removesuffix("`")
        #return jsonify({"error": "Output parsing error", "details": error_message}), 500
    except ValueError as e:
        # Handle any other ValueError that might be related to parsing
        error_message = str(e)
        print(f"ValueError caught: {error_message}", flush=True)
        match = re.search(r"Could not parse LLM output: `([^`]*)`", error_message)

        # Check if we found a match
        if match:
            extracted_message = match.group(1)  # This is "I don't know"
            return(extracted_message)
        else:
            return("I don't know")
        #return jsonify({"error": "ValueError", "details": error_message}), 500
    except Exception as e:
        # General exception handler for any unexpected exceptions
        error_message = str(e)
        print(f"Unexpected error caught: {error_message}", flush=True)
        #return jsonify({"error": "Unexpected error", "details": error_message}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5201)