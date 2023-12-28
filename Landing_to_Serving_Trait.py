#!/usr/bin/env python
# coding: utf-8

# ## Landing_to_Serving_Trait
# 
# 
# 

# In[152]:


get_ipython().run_cell_magic('pyspark', '', '\r\nfrom pyspark.sql import SparkSession\r\nfrom pyspark.sql.types import *\r\nfrom pyspark.sql.functions import *    \r\nfrom datetime import datetime, timedelta\r\nfrom delta.tables import *\r\nimport logging\r\nimport os\r\nimport io\r\nimport re\r\nimport pandas as pd\r\nfrom azure.storage.blob import BlobServiceClient, BlobClient\r\nfrom pyspark.sql.functions import regexp_replace\r\n\r\nspark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")\n')


# In[153]:


get_ipython().run_line_magic('run', 'Parameters/Parameters')


# In[154]:


get_ipython().run_cell_magic('pyspark', '', '\r\n\r\n#blob_account_name = "adldpwalmartdev"\r\n#blob_container_name = "dl-algo-edw"\r\n#client_name = "Walmart-US"\r\n\r\n\r\ncurrent_date = datetime.now().strftime("%Y-%m-%d")\r\nyesterday_date = ((datetime.now())-timedelta(days=1)).strftime("%Y-%m-%d")\r\n\r\nstg_File_Name = "Trait"\r\nserving_Path="DIM_TRAIT"\r\n\r\nblob_relative_path_landing = client_name + "/landing/" + current_date\r\nblob_relative_path_serving = client_name + "/serving/" + serving_Path\r\nblob_relative_path_serving_date = client_name + "/serving/DIM_DATE" \r\n\r\nsource_file_path = f"abfss://{blob_container_name}@{blob_account_name}.dfs.core.windows.net/"\r\ndestination_file_path = f"abfss://{blob_container_name}@{blob_account_name}.dfs.core.windows.net/{blob_relative_path_serving}/"\r\ndate_file_path = f"abfss://{blob_container_name}@{blob_account_name}.dfs.core.windows.net/{blob_relative_path_serving_date}/"\r\n\r\nconnection_string = "DefaultEndpointsProtocol=https;AccountName=adldpwalmartdev;AccountKey=qghGAEgyCBEH9eZtAW+zioPQ39qp5wU7ukItURfSObkVLDV75xry5aCrb0iLuPzlftqSo+KqnzuB+ASt1eEfvg==;EndpointSuffix=core.windows.net"\r\n\r\n#print(source_file_path)\r\n#print(destination_file_path)\r\n#print(date_file_path)\r\n#print(blob_relative_path_landing)\r\n')


# In[155]:


get_ipython().run_cell_magic('pyspark', '', '\r\n#FileWeekId=None\r\n#PREPROCESSING AND VALIDATING DATA\r\nblob_service_client = BlobServiceClient.from_connection_string(connection_string)\r\n\r\n# Get a reference to the container\r\ncontainer_client = blob_service_client.get_container_client(blob_container_name)\r\n\r\n# List blobs in the specified folder\r\nfile_names = container_client.list_blobs(name_starts_with=blob_relative_path_landing)\r\n\r\n\r\n\r\n# List blobs in the specified folder\r\nfile_names = container_client.list_blobs(name_starts_with=blob_relative_path_landing)\r\n\r\nresult_df = pd.DataFrame()\r\n\r\nfor file_name in file_names:\r\n    if file_name.name.endswith(\'.txt\') and \'traits\' in file_name.name.lower():\r\n        # Download the blob and read it into a DataFrame\r\n        file_path = os.path.join(source_file_path,  file_name.name)\r\n        #print (file_name.name)\r\n        # Extract the number using regular expressions\r\n        #pattern = re.compile(r\'Traits[ ]*[-\\(\\[]?[ ]*(\\d+)[ ]*[\\)\\]]?[ ]*\\.txt\')\r\n        #match = pattern.search(file_path)\r\n        #if match:\r\n            # Extract the digits from the matched group\r\n            #traits_number = match.group(1)\r\n            #FileWeekId=traits_number\r\n            #print("Trait File Name has been matched:", traits_number)\r\n        #else:\r\n            #print("Trait File Name Not matched.")\r\n\r\n\r\n        \r\n        df = spark.read.option("Delimiter","\\t").csv(file_path)  # Modify delimiter if needed\r\n        #print (df.count())\r\n        df=df.toPandas()\r\n        result_df = result_df.append(df, ignore_index=True)\r\n        #print(df.show())\r\n\r\n        \r\n\r\n#print(f"File Name Weekid-{FileWeekId}") \r\nprint(result_df.head())\n')


# In[156]:


# Option 2: Rename multiple columns

new_column_names = {'_c0': 'StoreCode', '_c1': 'TraitCode','_c2': 'TraitDescription', '_c3': 'EffectiveDate'}
result_df.rename(columns=new_column_names, inplace=True)


# In[157]:


# Convert string representations of lists to actual lists
result_df['TraitCode'] = result_df['TraitCode'].apply(lambda x: [int(code) for code in x.strip('[]').split(',')])
result_df['TraitDescription'] = result_df['TraitDescription'].apply(lambda x: [desc.strip().strip('"') for desc in x.strip('[]').split(',')])
result_df['EffectiveDate'] = result_df['EffectiveDate'].apply(lambda x: [date.strip().strip('"') for date in x.strip('[]').split(',')])

# Use explode to split the lists into separate rows
df_exploded = result_df.explode(['TraitCode', 'TraitDescription', 'EffectiveDate'])

# Reset the index
df_exploded.reset_index(drop=True, inplace=True)

# Display the resulting DataFrame
print(df_exploded.head())


# In[158]:


df_exploded['EDW_ACTIVITY_DATE_TIME'] = pd.to_datetime(datetime.now())
df_exploded['BATCH_ID'] =  Batchid  #None


# In[159]:


df_exploded=df_exploded[['BATCH_ID','StoreCode', 'TraitCode', 'TraitDescription', 'EffectiveDate','EDW_ACTIVITY_DATE_TIME']]
df_no_duplicates = df_exploded.drop_duplicates().reset_index(drop=True)
#df_no_duplicates['CurrentWeekendNumber'] = FileWeekId 


# In[160]:


print(df_no_duplicates.head())


# In[161]:


get_ipython().run_cell_magic('pyspark', '', "\r\ndf_date = spark.read.parquet(date_file_path).select('FiscalWeekNumber','FiscalWeekEndDate','Date').distinct()\r\nprint(df_date)\n")


# In[162]:


get_ipython().run_cell_magic('pyspark', '', '\r\n\r\ndf_no_duplicates = spark.createDataFrame(df_no_duplicates)\r\ndf_trait = df_no_duplicates.withColumn("EffectiveDate",date_format( unix_timestamp(col("EffectiveDate"), "M/dd/yyyy").cast("timestamp"), "yyyy-MM-dd"))\r\ndf_trait.createOrReplaceTempView(\'\'+"Traits")\r\ndf_date.createOrReplaceTempView(\'\'+"DIM_DATE")\n')


# In[163]:


get_ipython().run_cell_magic('sql', '', "select * from traits\r\nwhere effectivedate = '2022-10-06'\n")


# In[164]:


#%%pyspark

#resultdf = spark.sql('''Select 
#CAST(T.BATCH_ID AS VARCHAR(50)) AS BATCH_ID,
#D.FiscalWeekEndDate as dim_date,
#T.StoreCode,
#T.TraitCode,
#T.TraitDescription,
#T.EffectiveDate,
#D.Fiscalweekenddate ,
#T.EDW_ACTIVITY_DATE_TIME
#from Traits T
#left join DIM_DATE D
#ON T.EffectiveDate = D.DATE''')



# In[165]:


get_ipython().run_cell_magic('pyspark', '', "\r\nresultdf = spark.sql('''Select \r\nCAST(T.BATCH_ID AS VARCHAR(50)) AS BATCH_ID,\r\n--D.DATE as dim_date,\r\nCAST(T.StoreCode AS int) Storecode,\r\nT.TraitCode,\r\nT.TraitDescription,\r\nT.EffectiveDate,\r\nCAST(CURRENT_DATE() AS DATE) CURRENT_DATE,\r\nD.Fiscalweekenddate ,\r\nT.EDW_ACTIVITY_DATE_TIME\r\nfrom Traits T\r\nleft join DIM_DATE D\r\nON CURRENT_DATE() = D.DATE''')\r\ndisplay(resultdf.limit(5))\r\n")


# In[166]:


display(resultdf.select('fiscalweekenddate').distinct())


# In[167]:


selected_columns = resultdf.select(
    col("BATCH_ID"),
    col("StoreCode"),
    col("TraitCode"),
    col("TraitDescription"),
    col("EffectiveDate"),
    col("Fiscalweekenddate"),
    col("EDW_ACTIVITY_DATE_TIME")
)


# In[168]:


selected_columns.show(5)


# In[169]:


# Get distinct values of Fiscalweekenddate
distinct_dates = resultdf.select("Fiscalweekenddate").distinct().collect()

# Choose one date from the distinct dates (for example, the first one)
chosen_date = distinct_dates[0][0]
file_name = chosen_date.strftime("%Y-%m-%d")
file_name


# In[170]:


#resultdf = resultdf.withColumn("Effectivedate",date_format( unix_timestamp(col("EffectiveDate"), "M/dd/yyyy").cast("timestamp"), "yyyy-MM-dd"))


# In[171]:


display(selected_columns.limit(5))


# In[172]:


#%%pyspark


#resultdf = spark.sql('''Select 
#CAST(T.BATCH_ID AS VARCHAR(50)) AS BATCH_ID,
#CAST(T.StoreCode AS VARCHAR(50)) Storecode,
#T.TraitCode,
#T.TraitDescription,
#T.EffectiveDate,
#CAST(D.Fiscalweekenddate AS DATE) AS Fiscalweekenddate,
#T.EDW_ACTIVITY_DATE_TIME
#from Traits T
#left join DIM_DATE D
#ON T.EffectiveDate = D.Date''')
#display(resultdf.limit(5))


# In[173]:


destination_file_path_week=destination_file_path+"DATE="+file_name
destination_file_path_week


# In[176]:


selected_columns.coalesce(1).write.mode("overwrite").parquet(destination_file_path_week)
#selected_columns.write.option("header",True).partitionBy("fiscalweekenddate").mode("overwrite").parquet(destination_file_path_week) 


# In[175]:


#commond to stop the session 
#mssparkutils.session.stop()


# In[180]:


source_file_path


# In[190]:


get_ipython().run_cell_magic('pyspark', '', "\r\n# Read all Parquet files in the directory\r\nparquet_data = spark.read.load('abfss://dl-algo-edw@adldpwalmartdev.dfs.core.windows.net/Walmart-US/serving/DIM_TRAIT/**', format='parquet')\r\n# Show the schema and display the data\r\n#parquet_data.printSchema()\r\nparquet_data=parquet_data.createOrReplaceTempView('DIM_TRAIT')\r\ndf=spark.sql('''\r\nselect\r\nCAST(BATCH_ID AS VARCHAR(50)) AS BATCH_ID,\r\nCAST(StoreCode AS int) Storecode,\r\nCAST(TraitCode AS INT) TraitCode,\r\nCAST(TraitDescription AS VARCHAR(50)) TraitDescription,\r\nCAST(EffectiveDate AS DATE) EffectiveDate ,\r\nCAST(DATE_FORMAT(Fiscalweekenddate,'%Y-%m-%d') AS DATE)  fiscalweekenddate,\r\nCAST(EDW_ACTIVITY_DATE_TIME AS DATE) EDW_ACTIVITY_DATE_TIME\r\nfrom DIM_TRAIT\r\n''')\r\ndisplay(df.count())\r\ndisplay(df.limit(100))\r\n")


# In[178]:


blob_relative_path_landing

