-- Databricks notebook source
create database if not exists f1_processed
location '/mnt/formula1dl15/processed'

-- COMMAND ----------

desc database extended f1_processed;

-- COMMAND ----------


