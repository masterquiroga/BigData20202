CREATE DATABASE IF NOT EXISTS bigdata
COMMENT 'Base de datos para crear tables';
--indica la Base de datos a usar
USE bigdata;
drop table IF EXISTS cpostal_aux;
drop table IF EXISTS cpostal ;

--establece utf-8 como codificacion para que acepte mayusculas y minusculas

CREATE EXTERNAL TABLE IF NOT EXISTS cpostal_aux(
d_codigo VARCHAR(7) NOT NULL DISABLE,
d_asenta STRING,
d_tipo_asenta STRING,
D_mnpio STRING, 
d_estado STRING,
d_ciudad STRING,
d_CP  VARCHAR(10),
c_estado STRING,
c_oficina STRING,
c_CP VARCHAR(10),
c_tipo_asenta STRING,
c_mnpio STRING,
id_asenta_cpcons STRING,
d_zona STRING,
c_cve_ciudad STRING) 
COMMENT 'Tabla de codigos postales auxliar'
ROW FORMAT
--format con serde para la codifición
SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
--demilitador
WITH SERDEPROPERTIES('field.delim'='|','serialization.encoding'='latin1')
--txt como formato de entrada
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
--ROW FORMAT DELIMITED
--LINES TERMINATED BY '\n'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/bigdata/cpostal_aux'
TBLPROPERTIES('skip.header.line.count'='1');

LOAD DATA INPATH '/bigdata/CPdescarga.txt' INTO TABLE cpostal_aux;
SELECT * FROM cpostal_aux limit 1;

--sabemos los valores que va a tener una collumna 
CREATE EXTERNAL TABLE IF NOT EXISTS cpostal(
d_codigo VARCHAR(7) NOT NULL DISABLE,
d_asenta STRING,
d_tipo_asenta STRING,
D_mnpio STRING, 
d_ciudad STRING,
d_CP  VARCHAR(10),
c_estado STRING,
c_oficina STRING,
c_CP VARCHAR(10),
c_tipo_asenta STRING,
c_mnpio STRING,
id_asenta_cpcons STRING,
d_zona STRING,
c_cve_ciudad STRING) 
COMMENT 'Tabla de codigos postales'
PARTITIONED BY (d_estado STRING)
CLUSTERED BY (d_zona) INTO 2 BUCKETS
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES('field.delim'='|','serialization.encoding'='latin1')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
--ROW FORMAT DELIMITED
--LINES TERMINATED BY '\n'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/bigdata/cpostal'
TBLPROPERTIES('skip.header.line.count'='1');

--particiones dinamicas en tablas internas 
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;


SHOW TABLES;
DESCRIBE FORMATTED cpostal;
INSERT OVERWRITE TABLE cpostal PARTITION(d_estado) select d_codigo,d_asenta,d_tipo_asenta,D_mnpio,d_ciudad,d_CP,c_estado,c_oficina,c_CP,
c_tipo_asenta,c_mnpio,id_asenta_cpcons,d_zona,c_cve_ciudad,d_estado from cpostal_aux;


SHOW PARTITIONS cpostal;
SELECT COUNT(*) FROM cpostal;
SELECT *FROM cpostal limit 5;