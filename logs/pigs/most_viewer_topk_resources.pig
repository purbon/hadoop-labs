REGISTER '../target/logs-0.0.1-jar-with-dependencies.jar';
A = LOAD '/data/hadoop/apache/' USING com.purbon.hadoop.pig.ApacheLogStorage() AS ( host:chararray, stamp:chararray, method:chararray, 
                                                                                    url:chararray, protocol:chararray, code:chararray,
                                                                                    time:chararray );


G = GROUP A BY CONCAT(CONCAT(SUBSTRING(stamp,INDEXOF(stamp, '/',0)+1, INDEXOF(stamp, '/',INDEXOF(stamp, '/',0)+1)), '#'), url);

F = FOREACH G {
    HOSTS  = FOREACH A GENERATE host;
    UNIQUE = DISTINCT HOSTS;
   	GENERATE group, COUNT(UNIQUE);
 }
 
F = ORDER F BY $1 DESC; 
L = LIMIT F 10;

STORE F INTO '/data/hadoop/topk_visits' USING PigStorage('\t');
