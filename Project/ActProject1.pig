-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/viswa/M4_Project' AS (name:chararray,line:Chararray)USING PigStorage('\t') ;
-- Combine the words from the above stage
grpd = GROUP file BY names;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0, COUNT($1) As numlines;
ord = ORDER cntd BY numlines DESC;
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/viswa/ActProject1_results';