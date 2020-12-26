# Simple-Oracle-Data-Extractor-Using-Python-Multiprocessing
Dumps Oracle db data to disk.

# [cli_ora_spooler.py] (https://github.com/alexbuz/Oracle-Data-Extractor-Using-Python-Multiprocessing/blob/master/cli_ora_spooler.py)
 usage:
```
 time ./python ora_spooler.py -a 500000 -d ','
```





#simple.py

#It's ~4 times faster than sqlplus.


```
time ./python simple.py
 
real    7m53.436s
user    1m20.197s
sys     0m36.874s

du -h data_dump.dat
7.7G    dump.dat
```

##Using sqlplus

```
time sqlplus oats/manage@jc1lbiorc7/oradb1s @sql.sql>dump.dat

real    28m0.732s
user    10m41.801s
sys     7m13.879s
```


###sql.sql
```SQL
set head off page 0
SELECT MSG||'|'||ORDDATE||'|'||ORDID||'|'||ORDREV||'|'||RECVMPID||'|'||CLORDID||'|'||ORDTIME||'|'||METHOD_CODE||'|'||SEC||'|'||CMSSEC||'|'||SIDE||'|'||QTY||'|'||PX||'|'||TIF||'|'||FOK||'|'||IOC||'|'||ORDTYPE||'|'||PEG||'|'||RECV_TERM_ID||'|'||DIR||'|'||TMO||'|'||ISO||'|'||FIRMNAME||'|'||FIRMMPID||'|'||CXLTIME||'|'||CXLBY||'|'||CXLQTY||'|'||CXLLVSQTY||'|'||CXLTYPE||'|'||ORDER_CAPACITY||'|'||SOURCEID||'|'||SUBJECT||'|'||ALO||'|'||CRP||'|'||MQT||'|'||NH||'|'||OPO||'|'||BESTBID||'|'||BESTOFFER||'|'||NBBOSOURCE||'|'||NBBOLOOKUPDATE||'|'||NBBOLOOKUPTIME||'|'||PROCESSINGSEQNUM data FROM (select * from OATS.RSBMT_RW_MATCHIT_ORDERS_0);

exit;
```
