# Databricks notebook source
# MAGIC %md
# MAGIC #External Hive Metastore Setup helper for Hive 2.2+
# MAGIC The official documentation for Azure Databricks External Hive metastore is here: https://docs.microsoft.com/en-us/azure/databricks/data/metastores/external-hive-metastore
# MAGIC The documentation omitted steps on packaging the metastore db jdbc driver and initializing a new metastore.
# MAGIC 
# MAGIC General Steps on configuring an external Hive Metastore:
# MAGIC 1. Provision a metastore db - not handled by this notebook. New db will need to be initialized with schematool. Gather jdbc connection info - url, user and password
# MAGIC 1. Add dbuser and dbpassword into secret scope
# MAGIC 1. Verify network access from Databricks cluster
# MAGIC 2. Download hive version as needed
# MAGIC 3. Configure Hive Metastore with initscript or with cluster UI.
# MAGIC 
# MAGIC How to use this notebook:
# MAGIC * Provision new Hive metastore db or use an existing one - Gate the connection info and store the dbuser and dbpassword into a Databricks secret scope
# MAGIC * Run this notebook on a DBR 7.3 LTS+ cluster with the parameters. This will generate the cluster init script needed for clusters to connect to the external metastore. This will generate a cluster init script on `dbfs:/databricks/scripts/external-metastore-<hiveversion>.sh` where `<hiveversion>` is the `hiveversion`  parameter without the period.
# MAGIC   * Parameters:
# MAGIC     * dbhost - the host of the metastore db
# MAGIC     * dbport - the port of the metestore db
# MAGIC     * dbtype - mssql (Azure SQL), postgres, or mysql
# MAGIC     * jdbcurl - The JDBC url to your metastore DB. For example, `jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=howardtestmetastore` for a Azure SQL DB.
# MAGIC     * hiveversion - The Hive version you want for your external metastore. Make sure it's in `X.X.X` format.
# MAGIC     * initmetastore - For new metastore DB, yes to initialize to target Hive version with Hive schematool
# MAGIC     * secretscope - The name of the Databricks secret scope
# MAGIC     * dbuser_secretname - The secret name of of the secret scope for the db user
# MAGIC     * dbpassword_secretname - The secret name of of the secret scope for the db password
# MAGIC     * metastorejarpath - the path used to store the downloaded hive jar/libs in DBFS. Note this does not get copied to the driver or worker nodes. Example Parameter in JSON:
# MAGIC ```json
# MAGIC {
# MAGIC "metastorejarpath":"metastore_jars_310","initmetastore":"No","hiveversion":"3.1.0","jdbcurl":"jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=howardtestmetastore","dbhost":"oneenvsql.database.windows.net","dbuser_secretname":"dbuser","dbtype":"mssql","secretscope":"oetrta","dbport":"1433","dbpassword_secretname":"dbpassword"
# MAGIC }
# MAGIC ```
# MAGIC     * When starting a cluster, add cluster init script: dbfs:/databricks/scripts/external-metastore-XXX.sh 
# MAGIC       Envs to add:
# MAGIC       ```
# MAGIC       SQLUSER={secrets/<scoope>/<dbuser key>}
# MAGIC       SQLPASSWD={secrets/<scope>/<dbpassword key>}
# MAGIC       ```
# MAGIC 
# MAGIC Supported Hive metastore DBs:
# MAGIC * Azure SQL
# MAGIC * MySQL
# MAGIC * Postgres

# COMMAND ----------

#add notebook parameters
dbutils.widgets.text("dbhost", "oneenvsql.database.windows.net")
dbutils.widgets.text("dbport", "1433")
dbutils.widgets.text("jdbcurl", "jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=howardtestmetastore")
dbutils.widgets.text("secretscope", "oetrta")
dbutils.widgets.text("dbuser_secretname", "dbuser")
dbutils.widgets.text("dbpassword_secretname", "dbpassword")
dbutils.widgets.text("hiveversion", "2.3.7")
dbutils.widgets.text("metastorejarpath", "metastore_jars_2.3.7") #Path in dbfs to store the hive jars/libs. No need to specify /dbfs for sh commands
dbutils.widgets.dropdown("initmetastore", "Yes", ["Yes", "No"])
dbutils.widgets.dropdown("dbtype", "mssql", ["mssql", "mysql", "postgres"])

# COMMAND ----------

dbhost = dbutils.widgets.get("dbhost")
dbport = dbutils.widgets.get("dbport")
jdbcurl = dbutils.widgets.get("jdbcurl")
secretscope = dbutils.widgets.get("secretscope")
dbuser_secretname = dbutils.widgets.get("dbuser_secretname")
dbpassword_secretname = dbutils.widgets.get("dbpassword_secretname")
hiveversion = dbutils.widgets.get("hiveversion")
hiveversionclean = hiveversion.replace(".","")
metastorejarpath = dbutils.widgets.get("metastorejarpath") #Path in dbfs to store the hive jars/libs. No need to specify /dbfs for sh commands
initmetastore = dbutils.widgets.get("initmetastore")
dbtype = dbutils.widgets.get("dbtype")

jdbcdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
if dbtype == 'mysql':
  jdbcdriver = "org.mariadb.jdbc.Driver"
elif dbtype == 'postgres':
  jdbcdriver = "org.postgresql.Driver"

dbuser = dbutils.secrets.get(scope = secretscope, key = dbuser_secretname)
dbpassword = dbutils.secrets.get(scope = secretscope, key = dbpassword_secretname)

# COMMAND ----------

envcontent = f"""
DBHOST="{dbhost}"
DBPORT="{dbport}"
JDBCURL="{jdbcurl}"
JDBCDRIVER="{jdbcdriver}"
SECRETSCOPE="{secretscope}"
DBUSER_SECRETNAME="{dbuser_secretname}"
DBPASSWORD_SECRETNAME="{dbpassword_secretname}"
HIVEVERSION="{hiveversion}"
HIVEVERSIONCLEAN="{hiveversionclean}"
METASTOREJARPATH="{metastorejarpath}"
INITMETASTORE="{initmetastore}"
DBTYPE="{dbtype}"
SQLUSER="{dbuser}"
SQLPASSWD="{dbpassword}"

"""
text_file = open("/tmp/msenv.sh", "w")
n = text_file.write(envcontent)
text_file.close()

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /tmp/msenv.sh
# MAGIC . /tmp/msenv.sh

# COMMAND ----------

# DBTITLE 1,Test Connection from cluster to metastore
# MAGIC %sh -e
# MAGIC . /tmp/msenv.sh
# MAGIC nc -vzw5 $DBHOST $DBPORT

# COMMAND ----------

# MAGIC %md
# MAGIC #Download the Hive version
# MAGIC Determine the Hive version and DBR version.
# MAGIC 
# MAGIC This will download Hive 3.1 regardless of the Hive version used. Hive 3.1 schematool will be used to initialize the metastore db.
# MAGIC 
# MAGIC Hive 2.3.7 (Databricks Runtime 7.0 and above): set spark.sql.hive.metastore.jars to builtin.
# MAGIC 
# MAGIC For all other Hive versions, Azure Databricks recommends that you download the metastore JARs and set the configuration spark.sql.hive.metastore.jars to point to the downloaded JARs using the procedure described in Download the metastore jars and point to them.
# MAGIC 
# MAGIC Note:
# MAGIC `spark.sql.warehouse.dir` to change default warehouse dir
# MAGIC 
# MAGIC Add these to the Env tab when creating a cluster. 
# MAGIC ```
# MAGIC SQLUSER={{secrets/<myscope>/<secretname>}}
# MAGIC SQLPASSWD={{secrets/<myscope>/<secretname>}}
# MAGIC ```
# MAGIC For example:
# MAGIC ```
# MAGIC SQLUSER={{secrets/oetrta/dbuser}}
# MAGIC SQLPASSWD={{secrets/oetrta/dbpassword}}
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Download Hive 3.1 - Always download this version for metastore init
# MAGIC %sh
# MAGIC wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
# MAGIC tar -xvzf hadoop-2.7.2.tar.gz --directory /opt
# MAGIC wget https://archive.apache.org/dist/hive/hive-3.1.0/apache-hive-3.1.0-bin.tar.gz
# MAGIC tar -xvzf apache-hive-3.1.0-bin.tar.gz --directory /opt

# COMMAND ----------

# DBTITLE 1,Download and package the jdbc driver
# MAGIC %sh
# MAGIC . /tmp/msenv.sh
# MAGIC if [ $DBTYPE == "MySql" ]
# MAGIC then
# MAGIC   #uncomment the db driver needed
# MAGIC   #MariaDB driver (MySQL):
# MAGIC   wget https://downloads.mariadb.com/Connectors/java/connector-java-2.6.1/mariadb-java-client-2.6.1.jar
# MAGIC   cp mariadb-java-client-2.6.1.jar /opt/apache-hive-3.1.0-bin/lib
# MAGIC elif [ $DBTYPE == "Postgres" ]
# MAGIC then
# MAGIC   #Postgres driver:
# MAGIC   wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar
# MAGIC   cp postgresql-42.2.2.jar /opt/apache-hive-3.1.0-bin/lib
# MAGIC else
# MAGIC   #mssql/azuresql
# MAGIC   wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/8.2.2.jre8/mssql-jdbc-8.2.2.jre8.jar
# MAGIC   cp mssql-jdbc-8.2.2.jre8.jar /opt/apache-hive-3.1.0-bin/lib
# MAGIC fi

# COMMAND ----------

# MAGIC %md
# MAGIC Version of Hive that you want the external metastore to be - downloads the JARs and adds the database driver to the directory.  Skip this step if you're going to use metastore version 3.1.0.
# MAGIC 
# MAGIC ```
# MAGIC ```

# COMMAND ----------

# MAGIC %sh 
# MAGIC . /tmp/msenv.sh
# MAGIC if [ $HIVEVERSION != "3.1.0" ]
# MAGIC then
# MAGIC   wget https://archive.apache.org/dist/hive/hive-$HIVEVERSION/apache-hive-$HIVEVERSION-bin.tar.gz
# MAGIC   tar -xvzf apache-hive-$HIVEVERSION-bin.tar.gz --directory /opt
# MAGIC   #Also copy the jdbc driver downloaded from previous cmd
# MAGIC   if [ $DBTYPE == "mysql" ]
# MAGIC   then
# MAGIC     JDBCJAR=mariadb-java-client-2.6.1.jar
# MAGIC   elif [ $DBTYPE == "postgres" ]
# MAGIC   then
# MAGIC     JDBCJAR=postgresql-42.2.2.jar
# MAGIC   else
# MAGIC     JDBCJAR=mssql-jdbc-8.2.2.jre8.jar
# MAGIC   fi
# MAGIC   cp $JDBCJAR /opt/apache-hive-$HIVEVERSION-bin/lib
# MAGIC fi

# COMMAND ----------

# MAGIC %md
# MAGIC #Copy the hive jars to dbfs path

# COMMAND ----------

# MAGIC %sh
# MAGIC . /tmp/msenv.sh
# MAGIC MOD_METASTOREJARPATH=/dbfs/databricks/$METASTOREJARPATH
# MAGIC mkdir -p MOD_METASTOREJARPATH
# MAGIC cp -r /opt/apache-hive-$HIVEVERSION-bin/lib/. $MOD_METASTOREJARPATH
# MAGIC cp -r /opt/hadoop-2.7.2/share/hadoop/common/lib/. $MOD_METASTOREJARPATH

# COMMAND ----------

# MAGIC %fs mkdirs /databricks/scripts

# COMMAND ----------

# MAGIC %md
# MAGIC #Generate the Cluster Init Script

# COMMAND ----------


contents = f"""#!/bin/sh
# Loads environment variables to determine the correct JDBC driver to use.
source /etc/environment
# Quoting the label (i.e. EOF) with single quotes to disable variable interpolation.
cat << 'EOF' > /databricks/driver/conf/00-custom-spark.conf
[driver] {{
    # Hive specific configuration options.
    # spark.hadoop prefix is added to make sure these Hive specific options will propagate to the metastore client.
    # JDBC connect string for a JDBC metastore
    "spark.hadoop.javax.jdo.option.ConnectionURL" = "{jdbcurl}"


    # Driver class name for a JDBC metastore
    "spark.hadoop.javax.jdo.option.ConnectionDriverName" = "{jdbcdriver}"

    # Spark specific configuration options
    "spark.sql.hive.metastore.version" = "{hiveversion}"
    # Skip this one if <hive-version> is 0.13.x.
    "spark.sql.hive.metastore.jars" = "/dbfs/databricks/{metastorejarpath}/*"
    #"spark.sql.hive.metastore.jars" = "builtin"

    "spark.hadoop.datanucleus.fixedDatastore" = "true"
    "spark.hadoop.datanucleus.autoCreateSchema" = "false"

EOF

USERNAME="$SQLUSER"
PASSWORD="$SQLPASSWD"

# Add the metastore username and password separately since must use variable expansion to get the secret values
cat << EOF >> /databricks/driver/conf/00-custom-spark.conf
    "spark.hadoop.javax.jdo.option.ConnectionUserName" = "$USERNAME"
    "spark.hadoop.javax.jdo.option.ConnectionPassword" = "$PASSWORD"
    }}
EOF
"""    
   
dbutils.fs.put(
    file = f"/databricks/scripts/external-metastore-{hiveversionclean}.sh",
    contents = contents,
    overwrite = True
)

# COMMAND ----------

# MAGIC %sh
# MAGIC . /tmp/msenv.sh
# MAGIC ls /dbfs/databricks/scripts/external-metastore-$HIVEVERSIONCLEAN.sh
# MAGIC cat /dbfs/databricks/scripts/external-metastore-$HIVEVERSIONCLEAN.sh

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the metastore

# COMMAND ----------

# MAGIC %md
# MAGIC Initialize the metastore.  Make sure to specify your target metastore version number.  This uses the 3.1.0 version of the schematool so that it can reference a URL - older versions can only reference the local metastore.  Note that for the schematool for MySQL the username referenced here does not include the @server extention like will be seen below in the init scripts.
# MAGIC 
# MAGIC * MySQL:
# MAGIC * Connect string: jdbc:mysql://test-external-meta.mysql.database.azure.com:3306/mymetastore310
# MAGIC * DB Driver: org.mariadb.jdbc.Driver
# MAGIC * dbType for the schematool is mysql
# MAGIC * The username for the mysql type is just user
# MAGIC * Postgres:
# MAGIC * Connect string: jdbc:postgresql://pg-metastore-test.postgres.database.azure.com:5432/mymetastore310
# MAGIC * DB Driver: org.postgresql.Driver
# MAGIC * dbType for the schematool is postgres
# MAGIC * The username for the postgres type is user@hostname
# MAGIC * Azure SQL:
# MAGIC * Connect string: jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=mymetastore310
# MAGIC * DB Driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
# MAGIC * dbType for the schematool is mssql

# COMMAND ----------

# DBTITLE 1,Initialize Metastore - ingore error if target version is not 3.1.0
# MAGIC %sh
# MAGIC . /tmp/msenv.sh
# MAGIC export HIVE_HOME="/opt/apache-hive-3.1.0-bin"
# MAGIC export HADOOP_HOME="/opt/hadoop-2.7.2"
# MAGIC export DB_DRIVER="$JDBCDRIVER"
# MAGIC export HIVE_URL="$JDBCURL"
# MAGIC export HIVE_PASSWORD="$SQLPASSWD"
# MAGIC export HIVE_USER="$SQLUSER"
# MAGIC if [ $INITMETASTORE == "Yes" ]
# MAGIC then 
# MAGIC   if [[ $HIVEVERSION == 2.2* ]]
# MAGIC   then
# MAGIC     SCHEMATO="2.2.0"
# MAGIC   elif [[ $HIVEVERSION == 2.3* ]]
# MAGIC   then
# MAGIC     SCHEMATO="2.3.0"
# MAGIC   else
# MAGIC     SCHEMATO="3.1.0"
# MAGIC   fi
# MAGIC 
# MAGIC   # If the dry run looks correct then uncomment out the init and run it
# MAGIC   /opt/apache-hive-3.1.0-bin/bin/schematool -dbType $DBTYPE -url $HIVE_URL -passWord $HIVE_PASSWORD -userName $HIVE_USER -driver $DB_DRIVER -initSchemaTo $SCHEMATO -ifNotExists -dryRun --verbose
# MAGIC 
# MAGIC   # Init the schema
# MAGIC   if [ $? -eq 0 ]
# MAGIC   then
# MAGIC     /opt/apache-hive-3.1.0-bin/bin/schematool -dbType $DBTYPE -url $HIVE_URL -passWord $HIVE_PASSWORD -userName $HIVE_USER -driver $DB_DRIVER -initSchemaTo $SCHEMATO -ifNotExists --verbose
# MAGIC   fi
# MAGIC fi

# COMMAND ----------

# DBTITLE 1,OPTIONAL: Verify metastore version. Ok to see version mismatch if it's not 3.1.0
# MAGIC %sh
# MAGIC . /tmp/msenv.sh
# MAGIC export HIVE_HOME="/opt/apache-hive-3.1.0-bin"
# MAGIC export HADOOP_HOME="/opt/hadoop-2.7.2"
# MAGIC export DB_DRIVER="$JDBCDRIVER"
# MAGIC export HIVE_URL="$JDBCURL"
# MAGIC export HIVE_PASSWORD="$SQLPASSWD"
# MAGIC export HIVE_USER="$SQLUSER"
# MAGIC 
# MAGIC # validate that schema is initialized
# MAGIC /opt/apache-hive-3.1.0-bin/bin/schematool -dbType $DBTYPE -url $HIVE_URL -passWord $HIVE_PASSWORD -userName $HIVE_USER -driver $DB_DRIVER -info

# COMMAND ----------

# MAGIC %md
# MAGIC #Done
# MAGIC Configure clusters to use the generated init script and set the secrets in the cluster env:
# MAGIC 
# MAGIC * Point the cluster to the cluster init script. `file = f"/databricks/scripts/external-metastore-{hiveversionclean}.sh"`
# MAGIC * Set the secrets in the Env tab to pull the metastore secrets from the secret scope.
# MAGIC ```
# MAGIC SQLUSER={{secrets/<myscope>/<secretname>}}
# MAGIC SQLPASSWD={{secrets/<myscope>/<secretname>}}
# MAGIC ```

# COMMAND ----------

print (f"""
Setup using init script:

Cluster init script: dbfs:/databricks/scripts/external-metastore-{hiveversionclean}.sh
Envs to add:
SQLUSER={{secrets/{secretscope}/{dbuser_secretname}}}
SQLPASSWD={{secrets/{secretscope}/{dbpassword_secretname}}}
""")

print(f"""

Setup using the UI. Copy these to the Spark Conf in the cluster config:

    "spark.hadoop.javax.jdo.option.ConnectionURL" = "{jdbcurl}"
    "spark.hadoop.javax.jdo.option.ConnectionDriverName" = "{jdbcdriver}"
    "spark.sql.hive.metastore.version" = "{hiveversion}"
    "spark.sql.hive.metastore.jars" = "/dbfs/databricks/{metastorejarpath}/*"
    "spark.hadoop.datanucleus.fixedDatastore" = "true"
    "spark.hadoop.datanucleus.autoCreateSchema" = "false"
    "spark.hadoop.javax.jdo.option.ConnectionUserName" = "{{secrets/{secretscope}/{dbuser_secretname}}}"
    "spark.hadoop.javax.jdo.option.ConnectionPassword" = "{{secrets/{secretscope}/{dbpassword_secretname}}}"
""")
