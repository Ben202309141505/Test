# Databricks notebook source
pwdstring = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="dwhchinaadmin-secret")

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

database_host = "mssql-ddpcn-dev02-chinaeast2.database.chinacloudapi.cn"
database_port = "1433" 
database_name = "sqldb-ddpcn-dev01-chinaeast2"
table = "[dw_sfe].[Dim_Product]"
user = "dwhchinaadmin"
password = pwdstring

url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"

remote_table = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", table)
  .option("user", user)
  .option("password", password)
  .load()
)

display(remote_table)

