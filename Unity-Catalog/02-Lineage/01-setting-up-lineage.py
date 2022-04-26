# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Show Lineage for Delta Tables in Unity Catalog
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-global.png" style="float:right; margin-left:10px" width="600"/>
# MAGIC 
# MAGIC Unity Catalog captures runtime data lineage for any table to table operation executed on a Databricks cluster or SQL endpoint. Lineage operates across all languages (SQL, Python, Scala and R) and it can be visualized in the Data Explorer in near-real-time, and also retrieved via REST API.
# MAGIC 
# MAGIC Lineage is available at two granularity levels:
# MAGIC - Tables
# MAGIC - Columns
# MAGIC 
# MAGIC Lineage takes into account the Table ACLs present in Unity Catalog. If a user is not allowed to see a table at a certain point of the graph, its information are redacted, but she can still see that a upstream or downstream table is present.
# MAGIC 
# MAGIC ## Working with Lineage
# MAGIC 
# MAGIC No modifications are needed to the existing code to generate the lineage.
# MAGIC 
# MAGIC Requirements:
# MAGIC - Source and target tables must be registered in a Unity Catalog metastore to be eligible for lineage capture
# MAGIC - The data manipulation must be performed using Spark DataFrame language
# MAGIC - To view lineage, users must have the SELECT privilege on the table
# MAGIC 
# MAGIC Limitations
# MAGIC - Streaming operations are not yet supported
# MAGIC - Lineage will not be captured when data is written directly to files in cloud storage even if a table is defined at that location (eg spark.write.save(“s3:/mybucket/mytable/”) will not produce lineage)
# MAGIC - Lineage is not captured across workspaces (eg if a table A > table B transformation is performed in workspace 1 and table B > table C in workspace 2, each workspace will show a partial view of the lineage for table B)
# MAGIC - Lineage is computed on a 30 day rolling window, meaning that lineage will not be displayed for tables that have not been modified in more than 30 days ago

# COMMAND ----------

# MAGIC %md-sandbox ## 1/ Create the STORAGE CREDENTIAL
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-1.png" style="float:right; margin-left:10px" width="600"/>
# MAGIC 
# MAGIC The first step is to create the `STORAGE CREDENTIAL`.
# MAGIC 
# MAGIC To do that, we'll use Databricks Unity Catalog UI:
# MAGIC 
# MAGIC 1. Open the Data Explorer in DBSQL
# MAGIC 1. Select the "Storage Credential" menu
# MAGIC 1. Click on "Create Credential"
# MAGIC 1. Fill your credential information: the name and IAM role you will be using
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-cred.png" width="400"/>
