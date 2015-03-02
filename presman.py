#!/usr/bin/env python


#                                
# ___ _ _ ___  ___._ _ _ ___._ _ 
#| . \ '_> ._><_-<| ' ' <_> | ' |
#|  _/_| \___./__/|_|_|_<___|_|_|
#|_|                             
# presman - version 1.2
# Author: Luis Marques
# Oracle Resource Manager Monitor 


import cx_Oracle
import datetime
import sys
import getopt
import ConfigParser
from prettytable import from_db_cursor
from prettytable import PrettyTable
import os
import time
headerKeys=''

def headerText(con, connection_string):
	version = '1.2'
	print 'pResman ' + version +' - Oracle Resource Manager Monitor  - Luis Marques (http://lcmarques.com)'
	print 'Connected to: '+ connection_string +'\n'
	getDBRMinfo(con)

def headerCPU(refresh_rate):
	HEADER = '\033[94m'
	ENDC = '\033[0m'
	print HEADER + 'CPU Information by Consumer Group '+ENDC+' (Refresh time: '+str(refresh_rate)+' seconds : Oracle snapshot: 60 seconds) -\n'

def headerSessionIO(refresh_rate):
	HEADER = '\033[94m'
	ENDC = '\033[0m'
	print HEADER + 'Session I/O Information by Consumer Group '+ENDC+'(Refresh time: '+str(refresh_rate)+' seconds)\n'

def headerParallel(refresh_rate):
	HEADER = '\033[94m'
	ENDC = '\033[0m'
	print HEADER + 'Session Parallel Servers Information on Consumer Group '+ENDC+'(Refresh time: '+str(refresh_rate)+' seconds)\n'


#Define all available options 
def availableOptions():
	options=['cpu', 'session_io', 'parallel']
	return options

def validateOptions(option_file):
	if option_file in availableOptions():
		return 0
	else:
		return -1

def ConfigSectionMap(section):
	dict1 = {}
	Config = ConfigParser.ConfigParser()
	Config.read('config.ini')
	options = Config.options(section)
	for option in options:
		try:
			dict1[option] = Config.get(section, option)
			if dict1[option] == -1:
				DebugPrint("skip: %s" % option)
		except:
			print("E: Exception on reading  %s!" % option)
			dict1[option] = None
	return dict1

def readConnectionString():
	try:
		DBCon = ConfigSectionMap("presman")['connection_string']
 		return DBCon
 	except:
 		print 'E: Error reading database connection string! Check your config file.'

def readRefreshRate():
	try:
		refresh_time = ConfigSectionMap("presman")['refresh_rate']
		if int(refresh_time) >= 3:
			return int(refresh_time)
		else:
			return -1
	except:
 		print 'E: Error reading refresh rate! Check your config file.'



def readOptionFile():
	option = ConfigSectionMap("presman")['option']
	
	if (validateOptions(option) == 0): 
		return option
	else:
		print 'E: Invalid option in configuration file. Current available options are:',
		for i in availableOptions(): print i,
		sys.exit(1)



def writeFileOutput(filename, historical_data):
	try:
		f=open(filename, "w")
		f.write(headerKeys)
		for i in historical_data:
			f.write(i)
		f.close()
	except Exception, e:
		print 'E: Error writing output to file'
		print str(e)

def connectDB(connect_string):
	try:
		con = cx_Oracle.connect(connect_string)
		return con
	except cx_Oracle.DatabaseError as e:
		s = str(e)
		print 'E: Database connection error: ' + s
		sys.exit(2)

def runStatement(con, sql_text):
	cursor=con.cursor()
	sql = cursor.execute(sql_text)
	return cursor	

def getDBRMinfo(con):
	HEADER = '\033[92m'
	ENDC = '\033[0m'
	sql_text = '''select name, value from v$parameter where name in (\'resource_manager_plan\', \'resource_manager_cpu_allocation\') order by name'''
	cursor=runStatement(con, sql_text);
	result_query=cursor.fetchall()
	print HEADER + 'Database Resource Manager parameters:' +ENDC
	for j in result_query:
		print '> '+j[0]+':'+j[1]
	print ''


def resman_perf():
	sql_text = ''' 
WITH total_consumed_time AS (
SELECT inst_id, DECODE(SUM(cpu_consumed_time),0,1,SUM(cpu_consumed_time)) total_cpu_time  FROM gv$rsrcmgrmetric
group by inst_id
)
SELECT rs1.inst_id,
rs1.consumer_group_name CONSUMER_GROUP,
rs1.NUM_CPUS AS N_CPUS,
round(rs1.cpu_consumed_time/1000, 2) AS CONSUMED_CPU,
round((rs1.cpu_consumed_time * 100) / (select total_cpu_time from total_consumed_time tt where tt.inst_id = rs1.inst_id), 2) CONSUMED_CPU_PERCT,
rs1.cpu_wait_time/1000 AS THROTTLED_CPU,
rs1.CPU_UTILIZATION_LIMIT as UTILIZATION_LIMIT,
round((rs1.cpu_consumed_time/1000) / (60 * (select value from v$osstat where stat_name = 'NUM_CPUS')),2) ORACLE_CPU,
(select max(ROUND(c.value,2)) from V$SYSMETRIC_HISTORY c where rs1.begin_time = c.begin_time and c.metric_id = 2057) HOST_CPU
from gv$rsrcmgrmetric rs1
order by inst_id, consumer_group_name
'''
	return sql_text


def resman_sess_io():
	sql_text = '''
SELECT a.inst_id,
  gc.consumer_group NAME,
  count(1) as SESSIONS,
  SUM(a.small_read_megabytes) SM_READ_MB,
  SUM(a.large_read_megabytes) LM_READ_MB,
  SUM(a.small_write_megabytes) SM_WRITE_MB,
  SUM(a.large_write_megabytes) LM_WRITE_MB,
  SUM(a.small_read_megabytes+large_read_megabytes+small_write_megabytes+large_write_megabytes) TOTAL_IO_MB,
  ROUND(SUM(a.small_read_megabytes+large_read_megabytes+small_write_megabytes+large_write_megabytes) / (select SUM(small_read_megabytes+large_read_megabytes+small_write_megabytes+large_write_megabytes)  FROM v$RSRC_SESSION_INFO sess, DBA_RSRC_CONSUMER_GROUPS cg where sess.current_consumer_group_id = cg.consumer_group_id and cg.consumer_group NOT LIKE \'ORA$%\') * 100, 2) CONSUMED_IO_PERCENT
FROM GV$RSRC_SESSION_INFO a,
DBA_RSRC_CONSUMER_GROUPS gc
where gc.consumer_group_id = a.current_consumer_group_id
GROUP BY a.inst_id, gc.consumer_group
order by 1,2 '''

	return sql_text

def resman_sess_parallel():
	sql_text = '''
		select inst_id, name CONSUMER_GROUP, (PQ_ACTIVE_TIME/1000) PX_STMT_ACTIVE_SECS, CURRENT_PQS_ACTIVE PX_STMT, CURRENT_PQS_QUEUED PX_QUEUED_SESSIONS, PQ_QUEUED_TIME PX_SESSIONS_QUEUE_TIME, CURRENT_PQ_SERVERS_ACTIVE PX_SERVERS,
		PQ_QUEUE_TIME_OUTS PX_STMT_TIMEOUTS 
		from GV$RSRC_CONSUMER_GROUP
		order by 1,2
	'''
	return sql_text

#Returns the dictionary keys or values for writing to file.
#TODO: replace global var for a more intelligent way

def saveHistoricData(dict_cg):
	global headerKeys
	historical_data=[]
	date_now = datetime.datetime.now().strftime("%H:%M:%S")

	try:
		headerKeys='DATETIME,'+','.join(map(str, dict_cg.keys()))+'\n'
		cg_values = ','.join(map(str, dict_cg.values()))
		return date_now + ',' +cg_values+'\n'

	except Exception, e:
		print 'E: Error while saving historical data: '+str(e)
		sys.exit(1)



def showPrettyTable(con, resman_funct):
		col_names = []
	#try:
		
		cursor=runStatement(con, resman_funct)
		result_query=cursor.fetchall()
		for i in range(0, len(cursor.description)): col_names.append(cursor.description[i][0])
		ptable = PrettyTable(col_names)

		for r in result_query: ptable.add_row(r)

		return result_query, ptable
	#except:
	#	print "E: Error getting values or printing table to the screen!"
	#	sys.exit(1)

# position_for_plot is the index position in the table that you want to draw the plot
def showMyTableAndPlot(con, resman_funct, position_for_key, position_for_value, saveHistoric):
	rows_char = {}

	try:
		result_query, ptable = showPrettyTable(con, resman_funct)
		for j in result_query: rows_char[j[position_for_key]]=j[position_for_value]

		# print the table to the screen
		print ptable
		print ''
		
		# print the percentage bar to the screen
		for y in rows_char:
			value_chart=round(rows_char[y], position_for_key)
			print "{0:30} [{1:5}%]".format(y, str(value_chart)), int(round(value_chart,0))*'#'

		if (saveHistoric): 
			 return saveHistoricData(rows_char)

		print ''
	except Exception, e:
		print str(e)
		print "E: Error printing table or plot to the screen!"
		sys.exit(1)

# position_for_plot is the index position in the table that you want to draw the plot
def showMyTable(con, resman_funct, position_for_key, position_for_value, saveHistoric):
	rows_char = {}

	try:
		result_query, ptable = showPrettyTable(con, resman_funct)
		for j in result_query: rows_char[j[position_for_key]]=j[position_for_value]

		# print the table to the screen
		print ptable
		print ''
		
		if (saveHistoric): 
			 return saveHistoricData(rows_char)

		print ''
	except Exception, e:
		print str(e)
		print "E: Error printing table or plot to the screen!"
		sys.exit(1)


def help():
	print 'pResman - Oracle Resource Manager Monitor  - Luis Marques (http://lcmarques.com)'
	print './presman.py -m <measure_name> -o <output_file> -c <column_id> -p'
	print 'Available measures: cpu, parallel, session_io'

	print 'Example #1 - Measure CPU by Consumer Group and plot column with id 4: ./presman.py -m cpu -c 4 -p'
	print 'Example #2 - Measure Parallel by Consumer Group and output to file values in column with id 4: ./presman.py -m parallel -c 4 -o foobar.csv'


def cmdlineOpts():

	measure=''
	filename=''
	column_id=0
	plot=False
	help_opt=False
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'hm:o:c:p')
	except getopt.GetoptError as err:
		print(err)
		sys.exit()
	for o,a in opts:
		if o in ("-m"):
			measure=a
		elif o in ("-o"):
			filename=a
		elif o in ("-c"):
			column_id=a
		elif o in ("-p"):
			plot=True
		elif o in ("-h"):
			 help_opt=True
			 help()
			 sys.exit()

	if (measure == '') and (help_opt == False):
		print 'E: -m option is mandatory.'
		sys.exit()
	# if filename is specified by -o then -c is mandatory
	if (filename != '') and (column_id == 0):
		print 'E: -c option is required. Please specify the column id to output'
		sys.exit()

	# if plot is True make sure that -c is mandatory
	if (plot==True) and (column_id == 0):
		print 'E: -c option is required to plot your data. Make sure that you plot a percentage column'
		sys.exit()


        
	return measure, filename, column_id, plot


# measure = measurement name
# arg_file = filename output
# column_id = id of the column to output to the file
# plot = draw a plot?

def showMyScreen(measure, arg_file, column_id, plot):
	try:
		historical_data=[]
		connection_string = readConnectionString()

		print 'I: Connecting to ' + connection_string + ' ...'
		if readRefreshRate() >= 0:
			refresh_rate =  readRefreshRate()
			print 'I: Refresh rate is ' + str(refresh_rate) + ' seconds'
		else:
			print 'E: Refresh rate is invalid. Less than 2 seconds is not allowed'
			sys.exit(1)

		con = connectDB(connection_string)
		historical_data.append(headerKeys)

		while 1:
			
			os.system('cls' if os.name == 'nt' else 'clear')
			headerText(con, connection_string)
			
			# verify if output file is used on argv
			if arg_file != '': saveHD = True 
			else: saveHD = False

			#CPU query
			if (measure == 'cpu' or measure == 'CPU'):
				headerCPU(refresh_rate)
				if plot:
					c_value=showMyTableAndPlot(con, resman_perf(), 1, int(column_id), saveHD) 
				else: 
					c_value=showMyTable(con, resman_perf(), 1, int(column_id), saveHD)

				time.sleep(refresh_rate)

			#Session I/O query
			if (measure == 'session_io' or measure == 'SESSION_IO'):
				headerSessionIO(refresh_rate)
				if plot:
					c_value=showMyTableAndPlot(con, resman_sess_io(), 1, int(column_id), saveHD)
				else:
					c_value=showMyTable(con, resman_sess_io(), 1, int(column_id), saveHD)

				time.sleep(refresh_rate)

			 #Parallel query
			if (measure == 'parallel' or measure == 'PARALLEL'):
				headerParallel(refresh_rate)
				if plot:
					c_value=showMyTableAndPlot(con, resman_sess_parallel(), 1, int(column_id), saveHD)
				else:
					c_value=showMyTable(con, resman_sess_parallel(), 1, int(column_id), saveHD)

				time.sleep(refresh_rate)
				
			if arg_file != '': 
				historical_data.append(c_value)


	except KeyboardInterrupt:
		print("Ok ok, quitting")
		con.close()
		if ( arg_file != ''):
			print("Writing buffer to file "+ arg_file)
			# on quit write data do file
			writeFileOutput(arg_file, historical_data)
		print 'Bye!'
		sys.exit(1)


def main(argv):		
	measure, filename, column_id, plot = cmdlineOpts()
	showMyScreen(measure, filename, column_id, plot)		

if __name__ == "__main__":
	main(sys.argv[1:])

