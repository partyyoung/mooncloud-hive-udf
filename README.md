# mooncloud-hive-udf
> ADD JAR '[path]mooncloud-hive-udf-0.0.1-SNAPSHOT.jar';

### utils
* CREATE TEMPORARY FUNCTION split_cols AS 'net.mooncloud.hadoop.hive.ql.udf.generic.GenericUDTFSplit';

### base64
* CREATE TEMPORARY FUNCTION mc_base64 AS 'net.mooncloud.hadoop.hive.ql.udf.UDFBase64';
* CREATE TEMPORARY FUNCTION mc_unbase64 AS 'net.mooncloud.hadoop.hive.ql.udf.UDFUnbase64';

### latlon_distance
* CREATE TEMPORARY FUNCTION latlon_distance AS 'net.mooncloud.hadoop.hive.ql.udf.UDFLatLonDistance';

### md5
* CREATE TEMPORARY FUNCTION md5 AS 'net.mooncloud.hadoop.hive.ql.udf.UDFMd5';

### Longest Common Subsequence/Substring
* CREATE TEMPORARY FUNCTION lcs/lcs_subsequence AS 'net.mooncloud.hadoop.hive.ql.udf.generic.GenericUDFLongestCommonSubsequence';
* CREATE TEMPORARY FUNCTION lcs_substring AS 'net.mooncloud.hadoop.hive.ql.generic.UDFLongestCommonSubstring';

### RSA
* CREATE TEMPORARY FUNCTION rsa_keygen AS 'net.mooncloud.hadoop.hive.ql.udf.generic.GenericUDFRSAKeyGen';
* CREATE TEMPORARY FUNCTION rsa_sign AS 'net.mooncloud.hadoop.hive.ql.udf.UDFRSASign';
* CREATE TEMPORARY FUNCTION rsa_verify AS 'net.mooncloud.hadoop.hive.ql.udf.UDFRSAVerify';

### AES
* CREATE TEMPORARY FUNCTION aes_encode AS 'net.mooncloud.hadoop.hive.ql.udf.UDFAESEncode';
* CREATE TEMPORARY FUNCTION aes_decode AS 'net.mooncloud.hadoop.hive.ql.udf.UDFASEDecode';