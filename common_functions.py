# Databricks notebook source
# DBTITLE 1,to read sql table
# sqlScope is an Azure Key Vault-backed secret scope create in databricks, then the key parameter in:
# dbutils.secrets.get(scope="sql_things", key="password")
# 'key' refers to the secret name in Azure Key Vault.

def read_sqltbl(tablename):
    df=spark.read\
        .format('jdbc')\
            .option('url','jdbc:sqlserver://sqlserver94.database.windows.net:1433;database=sqlDB')\
            .option('user', dbutils.secrets.get(scope="sqlScope",key="sqlusername"))\
            .option('password',dbutils.secrets.get(scope="sqlScope",key="sqlpwd"))\
            .option('dbtable',tablename)\
            .load()
    return(df)

#url= azure sql db-> conenction strings-> JDBC

#Human World	Azure World
# User account	Human identity
# Service Principal	Application identity (databricks)



# COMMAND ----------

# DBTITLE 1,verify if keyvault is working fine.

dbutils.secrets.listScopes()
dbutils.secrets.list("sqlScope")

# COMMAND ----------

# DBTITLE 1,pass query during read
def exec_sqlQuery(sqlquery):
    df=spark.read\
        .format('jdbc')\
            .option('url','jdbc:sqlserver://sqlserver94.database.windows.net:1433;database=sqlDB')\
            .option('user', dbutils.secrets.get(scope="sqlScope",key="sqlusername"))\
            .option('password',dbutils.secrets.get(scope="sqlScope",key="sqlpwd"))\
            .option('query',sqlquery)\
            .load()
    return(df)

# COMMAND ----------

# DBTITLE 1,write the other table
def write_sqltbl(df,tablename,writemode):
    df.write\
        .mode(writemode)\
        .format('jdbc')\
            .option('url','jdbc:sqlserver://sqlserver94.database.windows.net:1433;database=sqlDB')\
            .option('user', dbutils.secrets.get(scope="sqlScope",key="sqlusername"))\
            .option('password',dbutils.secrets.get(scope="sqlScope",key="sqlpwd"))\
            .option('dbtable',tablename)\
            .save()
    return(df)


# COMMAND ----------

# MAGIC %md
# MAGIC