REGISTER '../target/logs-0.0.1-jar-with-dependencies.jar';
A = LOAD '/data/hadoop/apache/' USING com.purbon.hadoop.pig.ApacheLogStorage() AS ( host:chararray, stamp:chararray, method:chararray, 
                                                                                    url:chararray, protocol:chararray, code:chararray,
                                                                                    time:chararray );


G = GROUP A BY ToMilliSeconds(ToDate(SUBSTRING(stamp,0, INDEXOF(stamp, ':',0)), 'dd/MMM/yyyy'));

F = FOREACH G {
    HOSTS  = FOREACH A GENERATE host;
    UNIQUE = DISTINCT HOSTS;
   	GENERATE ToDate(group), COUNT(UNIQUE);
 }

STORE F INTO '/data/hadoop/pig_unique_visitors' USING PigStorage('\t');

