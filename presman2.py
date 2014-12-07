#!/usr/bin/env python


#                                
# ___ _ _ ___  ___._ _ _ ___._ _ 
#| . \ '_> ._><_-<| ' ' <_> | ' |
#|  _/_| \___./__/|_|_|_<___|_|_|
#|_|                             
# presman - version 1.0 
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

def headerText():
	version = '1.0'
	print 'pResman ' + version +' - Oracle Resource Manager Monitor  - Luis Marques (http://lcmarques.com)'	

def headerCPU(refresh_rate):
	HEADER = '\033[94m'
	ENDC = '\033[0m'
	print HEADER + 'CPU Information by Consumer Group '+ENDC+' (Refresh time: '+str(refresh_rate)+' seconds)\n'

def headerSessionIO(refresh_rate):
	HEADER = '\033[94m'
	ENDC = '\033[0m'
	print HEADER + 'Session I/O Information by Consumer Group '+ENDC+'(Refresh time: '+str(refresh_rate)+' seconds)\n'


#Define all available options 
def availableOptions():
	options=['cpu', 'session_io']
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
	options_all=[]
	option = ConfigSectionMap("presman")['option']
	
	if (validateOptions(option) == 0): 
		return option

	return options_all



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

def resman_perf():
	sql_text = ''' WITH sumcpu as (SELECT SUM(consumed_cpu_time) consumed_cpu_percent from v$rsrc_consumer_group 
		where name not like \'ORA$%\')  SELECT name, active_sessions, execution_waiters, requests, cpu_wait_time, cpu_waits, consumed_cpu_time, 
		round((consumed_cpu_time*100 / (select consumed_cpu_percent from sumcpu)), 2) consumed_cpu_percent, yields 
  		FROM v$rsrc_consumer_group where name not like \'ORA$%\'
  		order by name
  		'''
	return sql_text

def resman_sess_io():
	sql_text = '''
SELECT 
  gc.consumer_group NAME,
  count(1) as SESSIONS,
  a.state SESSION_STATE,
  SUM(a.small_read_megabytes) SM_READ_MB,
  SUM(a.large_read_megabytes) LM_READ_MB,
  SUM(a.small_write_megabytes) SM_WRITE_MB,
  SUM(a.large_write_megabytes) LM_WRITE_MB,
  SUM(a.small_read_megabytes+large_read_megabytes+small_write_megabytes+large_write_megabytes) TOTAL_IO_MB,
  ROUND(SUM(a.small_read_megabytes+large_read_megabytes+small_write_megabytes+large_write_megabytes) / (select SUM(small_read_megabytes+large_read_megabytes+small_write_megabytes+large_write_megabytes)  FROM v$RSRC_SESSION_INFO sess, DBA_RSRC_CONSUMER_GROUPS cg where sess.current_consumer_group_id = cg.consumer_group_id and cg.consumer_group NOT LIKE \'ORA$%\') * 100, 2) CONSUMED_IO_PERCENT
FROM V$RSRC_SESSION_INFO a,
DBA_RSRC_CONSUMER_GROUPS gc
WHERE gc.consumer_group NOT LIKE \'ORA$%\'
AND gc.consumer_group_id = a.current_consumer_group_id
GROUP BY gc.consumer_group, state 
order by gc.consumer_group, state '''

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

	
	try:
		
		cursor=runStatement(con, resman_funct)
		result_query=cursor.fetchall()
		for i in range(0, len(cursor.description)): col_names.append(cursor.description[i][0])
		ptable = PrettyTable(col_names)

		for r in result_query: ptable.add_row(r)

		return result_query, ptable
	except:
		print "E: Error getting values or printing table to the screen!"
		sys.exit(1)

# position_for_plot is the index position in the table that you want to draw the plot
def showMyTableAndPlot(con, resman_funct, position_for_plot, saveHistoric):
	rows_char = {}

	try:
		result_query, ptable = showPrettyTable(con, resman_funct)
		for j in result_query: rows_char[j[0]]=j[position_for_plot]

		# print the table to the screen
		print ptable
		
		# print the percentage bar to the screen
		for y in rows_char:
			value_chart=round(rows_char[y], 1)
			print "{0:30} [{1:5}%]".format(y, str(value_chart)), int(round(value_chart,0))*'#'

		if (saveHistoric): 
			 return saveHistoricData(rows_char)

		print ''
	except Exception, e:
		print str(e)
		print "E: Error printing table or plot to the screen!"
		sys.exit(1)




def showMyScreen():
	try:
		historical_data=[]
		option = readOptionFile()
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
			time.sleep(refresh_rate)
			os.system('cls' if os.name == 'nt' else 'clear')
			headerText()

			if (option == 'cpu' or option == 'CPU'):
				headerCPU(refresh_rate)
				c_value=showMyTableAndPlot(con, resman_perf(), 7, True)
				

			if (option == 'session_io' or option == 'SESSION_IO'):
				headerSessionIO(refresh_rate)
				c_value=showMyTableAndPlot(con, resman_sess_io(), 8, True)
				
			if (checkIfOutputArgv() != 1):
					historical_data.append(c_value)



	except KeyboardInterrupt:
		print("Ok ok, quitting")
		con.close()
		arg_file = checkIfOutputArgv()
		if ( arg_file != 1):
			print arg_file
			print("Writing buffer to file...")
			# on quit write data do file
			writeFileOutput(arg_file, historical_data)
		sys.exit(1)

def checkIfOutputArgv():
	if len(sys.argv) == 2:
		filename = sys.argv[1]
		return filename
	else:
		return 1


def main(argv):		
	showMyScreen()		

if __name__ == "__main__":
	main(sys.argv[1:])

