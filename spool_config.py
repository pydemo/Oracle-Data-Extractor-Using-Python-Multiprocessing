cfg=\
{'databases':{'DEV':'oats@jc1lbiorc7/oradb1s'},
 'nls_param_sets': {'set1':"""
NLS_DATE_FORMAT	= 'DD-MON-RR HH24:MI:SS'
NLS_TIMESTAMP_FORMAT	='DD-MON-RR HH24:MI:SS.FF3'
NLS_TIMESTAMP_TZ_FORMAT	='DD-MON-RR HH:MI:SS.FF3 TZH:TZM'
"""},
	'sql_spool':{
#extract definition start-----------------------------	
'data_dump.dat':{'from':'DEV', 'nls_params': 'set1', 'query':
"""
SELECT * 
  FROM OATS.RSBMT_RW_MATCHIT_ORDERS_0 WHERE rownum<10000;
"""	},
#end--------------------------------------------------
}}

###############
##SQL CONFIG EXAMPLE
###############
if 0:\
cfg=\
{'databases':{'DEV':'oats@jc1lbiorc7/oradb1s'},
 'nls_param_sets': {'set1':"""
NLS_DATE_FORMAT	= 'DD-MON-RR HH24:MI:SS'
NLS_TIMESTAMP_FORMAT	='DD-MON-RR HH24:MI:SS.FF3'
NLS_TIMESTAMP_TZ_FORMAT	='DD-MON-RR HH:MI:SS.FF3 TZH:TZM'
"""},
	'sql_spool':{
#extract definition start-----------------------------	
'data_dump.dat':{'from':'DEV', 'nls_params': 'set1', 'query':
"""
SELECT * 
  FROM OATS.RSBMT_RW_MATCHIT_ORDERS_0 WHERE rownum<10000;
"""	},
#end--------------------------------------------------
}}
