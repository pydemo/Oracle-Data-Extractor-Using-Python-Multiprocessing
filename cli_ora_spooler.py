import os, sys, imp, time
import logging
import gzip
import mmap
import traceback
import atexit
import pprint as pp
from copy import deepcopy
import cx_Oracle
from pprint import pprint

from optparse import OptionParser
try:
    # Python 3.4+
    if sys.platform.startswith('win'):
        import multiprocessing.popen_spawn_win32 as forking
    else:
        import multiprocessing.popen_fork as forking
except ImportError:
    import multiprocessing.forking as forking
if sys.platform.startswith('win'):
    # First define a modified version of Popen.
    class _Popen(forking.Popen):
        def __init__(self, *args, **kw):
            if hasattr(sys, 'frozen'):
                # We have to set original _MEIPASS2 value from sys._MEIPASS
                # to get --onefile mode working.
                os.putenv('_MEIPASS2', sys._MEIPASS)
            try:
                super(_Popen, self).__init__(*args, **kw)
            finally:
                if hasattr(sys, 'frozen'):
                    # On some platforms (e.g. AIX) 'os.unsetenv()' is not
                    # available. In those cases we cannot delete the variable
                    # but only set it to the empty string. The bootloader
                    # can handle this case.
                    if hasattr(os, 'unsetenv'):
                        os.unsetenv('_MEIPASS2')
                    else:
                        os.putenv('_MEIPASS2', '')

    # Second override 'Popen' class with our modified version.
    forking.Popen = _Popen
import multiprocessing

class SendeventProcess(multiprocessing.Process):
    def __init__(self, resultQueue):
        self.resultQueue = resultQueue
        multiprocessing.Process.__init__(self)
        self.start()

    def run(self):
        print ('SendeventProcess')
        self.resultQueue.put((1, 2))
        print ('SendeventProcess')

		
try:
    import cStringIO
except ImportError:
    import io as cStringIO
	
job_status={}

def create_symlink(from_dir, to_dir):
	if (os.name == "posix"):
		os.symlink(from_dir, to_dir)
	elif (os.name == "nt"):
		print (4,from_dir)
		os.system('mklink /J %s %s' % (to_dir, from_dir))
	else:
		log.error('Cannot create symlink. Unknown OS.', extra=d)
def unlink(dirname):
	if (os.name == "posix"):
		os.unlink(dirname)
	elif (os.name == "nt"):
		os.rmdir( dirname )
	else:
		log.error('Cannot unlink. Unknown OS.', extra=d)
e=sys.exit
total_size=0
DEBUG =1 
jobname='ora_data_spooler'
ts=time.strftime('%Y%m%d_%a_%H%M%S') #timestamp
print(0,ts)
HOME= os.path.dirname(os.path.abspath(__file__))

config_home = os.path.join(HOME,'config')
#output------------------------------------
latest_out_dir =os.path.join(HOME,'output','latest')
ts_out_dir=os.path.join(HOME,'output',ts)
latest_dir =os.path.join(HOME,'log','latest')
ts_dir=os.path.join(HOME,'log',ts)

done_file= os.path.join(ts_dir,'DONE.txt')
job_status_file=os.path.join(ts_dir,'%s.%s.status.py' % (os.path.splitext(__file__)[0],jobname))	

d = {'iteration': 0, 'pid':0, 'rows':0}
FORMAT = '|%(asctime)-15s|%(pid)-5s|%(iteration)-2s|%(rows)-9s|%(message)-s'
if not DEBUG:	
	logging.basicConfig(filename=os.path.join(ts_dir,'%s.log' % jobname),level=logging.DEBUG,format=FORMAT)
else:
	logging.basicConfig(level=logging.DEBUG,format=FORMAT)

log = logging.getLogger(jobname)

def import_module(filepath):
	class_inst = None
	mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])
	assert os.path.isfile(filepath), 'File %s does not exists.' % filepath
	if file_ext.lower() == '.py':
		py_mod = imp.load_source(mod_name, filepath)

	elif file_ext.lower() == '.pyc':
		py_mod = imp.load_compiled(mod_name, filepath)
	return py_mod

def convertSize(size):
	if (size == 0):
		return '0B'
	size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
	i = int(math.floor(math.log(size,1024)))
	p = math.pow(1024,i)
	s = round(size/p,2)
	return '%s %s' % (s,size_name[i])
def chunks(cur): # 65536
	while True:
		rows=cur.fetchmany()
		if not rows: break;
		yield rows
def get_nls_params(cfg, spool_spec):
	assert 'nls_param_sets' in cfg.keys(), "'nls_param_sets' section is missing in config."
	assert 'nls_params' in spool_spec.keys(), "'nls_params' definition is missing in spool specification."
	assert spool_spec['nls_params'] in cfg['nls_param_sets'].keys(), 'nls_param_set "%s" is missing in nls_param_sets configuration.' % spool_spec['nls_params']
	return cfg['nls_param_sets'][spool_spec['nls_params']]
def extract_query_data(data):
	global log 
	id, query, opt=data	
	status=1
	conn,fn,q, nls  = query
	
	con = cx_Oracle.connect(conn)
	cur = con.cursor()
	nls_cmd="ALTER SESSION SET %s" % ' '.join(nls.split())
	cur.execute(nls_cmd)
	cur.execute('SELECt * FROM (%s) WHERE 1=2' % q)
	sel= 'SELECT ' + ("||'%s'||" % opt.column_delimiter). join([d[0] for d in cur.description]) + ' data   FROM ( %s)' % q 
	cur.arraysize=opt.array_size
	cur.execute(sel)
	cnt=0
	
	d = {'iteration': 0,'pid':os.getpid(), 'rows':0}
	if opt.compress:
		fn='%s.gz' % fn
		with gzip.open(fn, 'wb') as f_out:	
			for i, chunk  in enumerate(chunks(cur)):
				d['iteration']=i
				cnt+=len(chunk)
				d['rows']=cnt
				try:
					
					#log.info('Starting ' + multiprocessing.current_process().name, extra=d)
					f_out.write('\n'.join([row[0] for row in chunk]))
					log.info('extracted into %s' % os.path.basename(fn), extra=d )

				except Exception as err:
					tb = traceback.format_exc()
					print (tb)
					exc_type, exc_obj, exc_tb = sys.exc_info()
					fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
					print(exc_type, fname, exc_tb.tb_lineno)
					raise
	else:	
		with open(fn, 'wb') as fh:
			fh.seek(0)
			for i, chunk  in enumerate(chunks(cur)):
				d['iteration']=i
				cnt+=len(chunk)
				d['rows']=cnt
				fh.write('\n'.join([row[0] for row in chunk]))

				log.info('extracted into %s' % os.path.basename(fn), extra=d )
				
	status=0
	
	cur.close()
	con.close()
	return (cnt, fn, status)
def save_status():
	global job_status_file
	p = pp.PrettyPrinter(indent=4)
	with open(job_status_file, "w") as text_file:
		cfg= deepcopy(config.cfg)
		text_file.write('cfg=%s\nstatus=%s' % (p.pformat(cfg),p.pformat(job_status)))
		print (job_status_file)		
def start_process():
	log.info('Starting ' + multiprocessing.current_process().name, extra=d)

def get_source_db_connect_string(cfg, spool_spec):
	global jobname
	assert 'databases' in cfg.keys(), "'databases' section is missing in config."
	assert 'from' in spool_spec.keys(), "'from' definition is missing in spool specification."
	assert spool_spec['from'] in cfg['databases'].keys(), 'database "%s" is missing in "databases" configuration.' % spool_spec['from']
	cli_var_name='%s0%s0%s' % (jobname,'databases', spool_spec['from'])
	#pprint (os.environ.keys())
	assert cli_var_name.upper() in [x.upper() for x in os.environ.keys()] , 'Source db password is not set.\nUse "set %s=<passwd>".' % cli_var_name
	conn = cfg['databases'][spool_spec['from']].split('@')
	assert len(conn)==2, 'Wrong connector format. Should be "user@dbserver/SID"'
	pwd=os.environ[cli_var_name]
	
	return  ('/%s@' % pwd). join (conn)
	
max_pool_size=multiprocessing.cpu_count() * 2
if __name__ == '__main__':
	multiprocessing.freeze_support()
	if not os.path.exists(ts_out_dir):
		print (2,ts_out_dir)
		os.makedirs(ts_out_dir)
	
	if  os.path.exists(latest_out_dir):
		unlink(latest_out_dir)
	#os.symlink(ts_out_dir, latest_out_dir)
	create_symlink(ts_out_dir, latest_out_dir)
	#log------------------------------------


	if not os.path.exists(ts_dir):
		print (1,ts_dir)
		os.makedirs(ts_dir)
		
	if  os.path.exists(latest_dir):	
		unlink(latest_dir)
	create_symlink(ts_dir, latest_dir)	
	parser = OptionParser()
	parser.add_option("-c", "--spool_config", dest="spool_config", type=str, default='spool_config.py')
	parser.add_option("-p", "--pool_size", dest="pool_size", type=int, default=multiprocessing.cpu_count() * 2)
	parser.add_option("-a", "--array_size", dest="array_size", type=int, default=100000)
	parser.add_option("-d", "--column_delimiter", dest="column_delimiter", type=str, default='|')
	parser.add_option("-u", "--compress",  action="store_true", dest="compress", default=False,
                  help="Compress extracted files")
	#parser.add_option("-d", "--spool_to_dir",   dest="spool_to_dir", type=str, default='.',
    #             help="Extract to directory.")				  
	opt= parser.parse_args()[0]

	
	config_file = os.path.join(config_home,opt.spool_config)

	if opt.pool_size> max_pool_size:	
		pool_size=max_pool_size
		log.warn('pool_size value is too high. Setting to %d (cpu_count() * 2)' % max_pool_size)
	else:
		pool_size=opt.pool_size

	queries=[]
	

	
	#__builtin__.trd_date = None
			
	config=import_module(config_file)

	for k,v in config.cfg['sql_spool'].items():
		q= v['query'].strip().strip('/').strip(';').strip()
		
		fn=os.path.join(ts_out_dir,k)
		nls=get_nls_params(config.cfg,v)
		#get password from environment
		conn = get_source_db_connect_string(config.cfg,v)
		#print conn
		#e(0)
		queries.append([conn,fn,q,nls])
	
	m= multiprocessing.Manager()
	if pool_size>len(queries):
		pool_size=len(queries)
	inputs = list([(i,q, opt) for i,q in enumerate(queries)])

	pool = m.Pool(processes=pool_size,
								initializer=start_process,
								)
	pool_outputs = pool.map(extract_query_data, inputs)
	pool.close() # no more tasks
	pool.join()  # wrap up current tasks

	#print ('Pool    :', pool_outputs)
	#e(0)
	print  ('Total rows extracted    : %d' % sum([r[0] for r in pool_outputs]))
	job_status={'spool_status':[r[2] for r in pool_outputs],'spool_files':[r[1] for r in pool_outputs]}
	print ('#'*60)
	for r in pool_outputs:
		print (r[2],r[1])
	print ('#'*60)
	
	atexit.register(save_status)	
