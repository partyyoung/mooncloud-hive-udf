# mooncloud-hive-udf
> ADD JAR '[path]mooncloud-hive-udf-0.0.1-SNAPSHOT.jar';

### base64
* CREATE TEMPORARY FUNCTION mc_base64 AS 'net.mooncloud.hadoop.hive.ql.udf.UDFBase64';
* CREATE TEMPORARY FUNCTION mc_unbase64 AS 'net.mooncloud.hadoop.hive.ql.udf.UDFUnbase64';

### latlon_distance
* CREATE TEMPORARY FUNCTION latlon_distance AS 'net.mooncloud.hadoop.hive.ql.udf.UDFLatLonDistance';

### md5
* CREATE TEMPORARY FUNCTION md5 AS 'net.mooncloud.hadoop.hive.ql.udf.UDFMd5';

### most common subsequence
* CREATE TEMPORARY FUNCTION mostCommonSubsequence AS 'net.mooncloud.hadoop.hive.ql.udf.generic.GenericUDFMostCommonSubsequence';

### RSA
* CREATE TEMPORARY FUNCTION rsa_keygen AS 'net.mooncloud.hadoop.hive.ql.udf.generic.GenericUDFRSAKeyGen';
* CREATE TEMPORARY FUNCTION rsa_sign AS 'net.mooncloud.hadoop.hive.ql.udf.UDFRSASign';
* CREATE TEMPORARY FUNCTION rsa_verify AS 'net.mooncloud.hadoop.hive.ql.udf.UDFRSAVerify';