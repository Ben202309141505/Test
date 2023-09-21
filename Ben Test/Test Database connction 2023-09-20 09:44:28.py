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


# COMMAND ----------

pwdstring = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="dwhchinaadmin-secret")

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

connect_string2 = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="dwhchinaadmin-connect")

connect_string2.format(password=pwdstring)

connect_string = "jdbc:sqlserver://mssql-ddpcn-dev02-chinaeast2.database.chinacloudapi.cn:1433;database=sqldb-ddpcn-dev01-chinaeast2;user=dwhchinaadmin;password={password}".format(password=pwdstring)

table = "[dw_sfe].[Dim_Product]"

remote_table = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", connect_string)
  .option("dbtable", table)
  .load()
)

display(remote_table)


# COMMAND ----------

#%fs mkdirs /tmp/config/
%fs ls /tmp/
