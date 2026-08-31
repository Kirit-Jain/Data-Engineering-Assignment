# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC select * from cyntexa_dev.bronze.bronze_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists cyntexa_dev.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cyntexa_dev.silver.silver_sales;

# COMMAND ----------

# MAGIC %sql drop table if exists cyntexa_dev.silver.silver_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists cyntexa_dev.gold;

# COMMAND ----------

# MAGIC %sql select * from cyntexa_dev.gold.daily_revenue_by_store