<center>
<h3>Tutorial</h3>
<h4> Tabela Externa com arquivo Parquet </h4>
</center>

Neste tutorial iremos aprender como criar uma tabela externa (Hive) no S3, utilizando Spark (PySpark), fazendo com que o Hive leia os dados por um arquivo parquet (salvo em um bucket do S3).

- 1º Criando a tabela no Hive.

Inicie o Hive e crie a tabela como o exemplo abaixo.
```sql
CREATE TABLE banco.tabela(nome_coluna VARCHAR(50))
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' stored as
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
location 's3://meu_bucket/tabela.parquet';
```
Após criado a tabela, inicie o Spark.
```
pyspark --conf spark.executor.extraClassPath=/usr/share/java/ojdbc8.jar:/usr/share/aws/emr/emrfs/lib/* --jars /usr/share/java/ojdbc8.jar --conf "spark.sql.parquet.writeLegacyFormat=true"
```
No exemplo a cima estamos iniciando o Spark, passando alguns parametros de inicialização, como ClassPath e alguns JDBC's para buscar uma tabelas ja existentes por exemplo. 
Obs 1: A conexão vai variar de banco pra banco.
Obs 2: O parametro  ```--conf "spark.sql.parquet.writeLegacyFormat=true"``` foi adcionado para evitar um erro na leitura do parquet. Existe uma diferença em relação a gravação, veja em mais ![](https://stackoverflow.com/questions/37829334/parquet-io-parquetdecodingexception-can-not-read-value-at-0-in-block-1-in-file)

Depois de iniciado, agora iremos executar o código para a cópia de uma tabela, e gravação de um arquivo parquet.

```python
import time
import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,unix_timestamp

#inicia a sessao
sparkSession = (SparkSession.builder.appName('IT4RAW').enableHiveSupport().getOrCreate())

appName = "IT4RAW"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master).set("spark.driver.extraClassPath","/usr/share/java/ojdbc8.jar")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession


conn = "jdbc:oracle:thin:@IP_DO_BANCO"

# Oracle para Data Frame 
## O código irá pegar dados de uma tabela chamada "tabela_origem" e passar os dados dessa tabela parar um Data Frame
df = sqlContext.read.format("jdbc").option("driver", "oracle.jdbc.driver.OracleDriver").option("url", conn).option("dbtable", "tabela_origem").option("user", "nome_do_usuario").option("password", "senha_do_usuario").load()

#Cria uma coluna com data atual
timestamp = (datetime.datetime.fromtimestamp(time.time()) - datetime.timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')

# Adcionar coluna de data atual ao Data Frame
new_df = df.withColumn('current_time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

# Gravar Parquet apartir de um DataFrame
new_df.write.parquet('s3://meu_bucket/tabela.parquet')
```
Após rodar o código, o parquet será criado na pasta indicada.

Obs 3: Faça um teste e de um "Select" no banco,  verifique se o Hive está pegando os dados do parquet e populando sua tabela.
