import os, sys
from pprint import pprint
import cx_Oracle
e=sys.exit
con = cx_Oracle.connect('test/manage@server/oradb1')
cur = con.cursor()
q= 'select * from OATS.TESTDATA_0'
cur.execute('SELECt * FROM (%s) WHERE 1=2' % q)
pprint(dir(cur))
sel= 'SELECT ' + "||'|'||". join([d[0] for d in cur.description]) + ' data   FROM ( %s)' % q 
print sel
#cur.close()
cur.arraysize=500000
cur.execute(sel)
#e(0)
out =[]
i=0
with open('data_dump.dat', 'wb') as fh:
	while True:
		rows=cur.fetchmany()
		#print rows
		#e(0)
		if not rows: break;
		fh.write('\n'.join([row[0] for row in rows]))
		print len(rows)
	

cur.close()
con.close()





