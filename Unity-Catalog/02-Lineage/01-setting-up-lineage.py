# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Show Lineage for Delta Tables in Unity Catalog
# MAGIC 
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
# MAGIC <img src="https://github.com/mattiazenidb/databricks-demo-uc-lineage/blob/master/Unity-Catalog/02-Lineage/Images/initial.png?raw=true" style="float:right; margin-left:10px" width="1200"/>
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

# MAGIC %md-sandbox ## 1/ Create a Delta Table In Unity Catalog
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-1.png" style="float:right; margin-left:10px" width="600"/>
# MAGIC 
# MAGIC The first step is to create a Delta Table in Unity Catalog.
# MAGIC 
# MAGIC We want to do that in SQL, to show multi-language support:
# MAGIC 
# MAGIC 1. Use the `CREATE TABLE` command and define a schema
# MAGIC 1. Use the `INSERTO INTO` command to insert some rows in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS main.lineage.menu (
# MAGIC   recipe_id INT,
# MAGIC   app string,
# MAGIC   main string,
# MAGIC   desert string
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO main.lineage.menu 
# MAGIC     (recipe_id, app, main, desert) 
# MAGIC VALUES 
# MAGIC     (1,"Ceviche", "Tacos", "Flan"),
# MAGIC     (2,"Tomato Soup", "Souffle", "Creme Brulee"),
# MAGIC     (3,"Chips","Grilled Cheese","Cheescake");

# COMMAND ----------

# MAGIC %md-sandbox ## 2/ Create a Delta Table from the Previously Created One
# MAGIC 
# MAGIC To show dependancies between tables, we create a new one `AS SELECT` from the previous one, concatenating three columns into a new one

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE main.lineage.dinner 
# MAGIC AS SELECT recipe_id, concat(app," + ", main," + ",desert) as full_menu FROM main.lineage.menu

# COMMAND ----------

# MAGIC %md-sandbox ## 3/ Create a Delta Table as join from Two Other Tables
# MAGIC 
# MAGIC The last step is to create a third table as a join from the two previous ones. This time we will use Python instead of SQL.
# MAGIC 
# MAGIC - We create a Dataframe with some random data formatted according to two columns, `id` and `recipe_id`
# MAGIC - We save this Dataframe as a new table, `main.lineage.price`
# MAGIC - We read as two Dataframes the previous two tables, `main.lineage.dinner` and `main.lineage.price`
# MAGIC - We join them on `recipe_id` and save the result as a new Delta table `main.lineage.dinner_price`

# COMMAND ----------

from pyspark.sql.functions import rand, round

df = spark.range(3).withColumn("price", round(10*rand(seed=42),2)).withColumnRenamed("id", "recipe_id")

df.write.mode("overwrite").saveAsTable("main.lineage.price")

dinner = spark.read.table("main.lineage.dinner")
price = spark.read.table("main.lineage.price")

dinner_price = dinner.join(price, on="recipe_id")
dinner_price.write.mode("overwrite").saveAsTable("main.lineage.dinner_price")


# COMMAND ----------

# MAGIC %md-sandbox ## 4/ Visualize the Lineage
# MAGIC 
# MAGIC The Lineage can be visualized in the Data Explorer of the part of the Workspace dedicated to the SQL Persona.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-cred.png" width="400"/>

# COMMAND ----------

# MAGIC %md-sandbox ## 5/ Table ACLs and Lineage Redaction
# MAGIC 
# MAGIC To visualize this first table in the Lineage View, go to the part of the Workspace dedicated to the SQL Persona, then
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-cred.png" width="400"/>
