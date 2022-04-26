-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS main.lineage

-- COMMAND ----------

DROP TABLE IF EXISTS main.lineage.menu;
DROP TABLE IF EXISTS main.lineage.dinner;
DROP TABLE IF EXISTS main.lineage.dinner_price;
DROP TABLE IF EXISTS main.lineage.price;
