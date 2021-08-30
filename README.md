# External Hive Metastore Setup helper for Hive 2.2+
This notebook helps simplify and automate the external Hive metastore setup on Databricks.

The official documentation for Azure Databricks External Hive metastore is here: https://docs.microsoft.com/en-us/azure/databricks/data/metastores/external-hive-metastore
The documentation omitted steps on packaging the metastore db jdbc driver and initializing a new metastore.

General Steps on configuring an external Hive Metastore:
1. Provision a metastore db - not handled by this notebook. New db will need to be initialized with schematool. Gather jdbc connection info - url, user and password
1. Add dbuser and dbpassword into secret scope
1. Verify network access from Databricks cluster
2. Download hive version as needed
2. Init hive metatore store db as needed
3. Configure and setup Hive Metastore with initscript for regular clusters or with cluster UI for interactive clusters and DB Sql.

How to use this notebook:
* Provision new Hive metastore db or use an existing one - Gate the connection info and store the dbuser and dbpassword into a Databricks secret scope
* Run this notebook on a DBR 7.3 LTS+ cluster with the parameters either interactively or as a job. This will generate the cluster init script and configurations needed for clusters to connect to the external metastore. Check the last command results to get the setup information.
  * Parameters:
    * dbhost - the host of the metastore db
    * dbport - the port of the metestore db
    * dbtype - mssql (Azure SQL), postgres, or mysql
    * jdbcurl - The JDBC url to your metastore DB. For example, `jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=howardtestmetastore` for a Azure SQL DB.
    * hiveversion - The Hive version you want for your external metastore. Make sure it's in `X.X.X` format.
    * initmetastore - For new metastore DB, yes to initialize to target Hive version with Hive schematool
    * secretscope - The name of the Databricks secret scope
    * dbuser_secretname - The secret name of of the secret scope for the db user
    * dbpassword_secretname - The secret name of of the secret scope for the db password
    * metastorejarpath - the path used to store the downloaded hive jar/libs in DBFS. Note this does not get copied to the driver or worker nodes. 

Example parameter in JSON:
```json
{
"metastorejarpath":"metastore_jars_310","initmetastore":"No","hiveversion":"3.1.0","jdbcurl":"jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=howardtestmetastore","dbhost":"oneenvsql.database.windows.net","dbuser_secretname":"dbuser","dbtype":"mssql","secretscope":"oetrta","dbport":"1433","dbpassword_secretname":"dbpassword"
}
```

When starting a cluster, add cluster init script: `dbfs:/databricks/scripts/external-metastore-XXX.sh`

Envs to add:

```
SQLUSER={secrets/<scoope>/<dbuser key>}
SQLPASSWD={secrets/<scope>/<dbpassword key>}
```

Supported Hive metastore DBs:
* Azure SQL
* MySQL
* Postgres
