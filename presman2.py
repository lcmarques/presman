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
import sys
import getopt
import ConfigParser
from prettytable import from_db_cursor
from prettytable import PrettyTable
import os
import time

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
	DBCon = ConfigSectionMap("presman")['connection_string']
 	return DBCon

def readRefreshRate():
	refresh_time = ConfigSectionMap("presman")['refresh_rate']
	if int(refresh_time) >= 2:
		return int(refresh_time)
	else:
		return -1


def validateOptions(option_file):
	if (option_file == 'cpu') or (option_file=='session_io'):
		return 0
	else:
		return -1

def readOptionFile():
	options_all=[]
	option_1 = ConfigSectionMap("presman")['option_1']
	option_2 = ConfigSectionMap("presman")['option_2']

	if (validateOptions(option_1) == 0): 
		options_all.append(option_1)
	if (validateOptions(option_2) == 0): 
		options_all.append(option_2)
	return options_all
	#else:
	#	return -1

def readOption_2():
	option_2 = ConfigSectionMap("presman")['option_1']
	return option_2

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
	sql_text = 'WITH sumcpu as (SELECT SUM(consumed_cpu_time) consumed_cpu_percent from v$rsrc_consumer_group where name not like \'ORA$%\')  SELECT name, active_sessions, execution_waiters, requests, cpu_wait_time, cpu_waits, consumed_cpu_time, round((consumed_cpu_time*100 / (select consumed_cpu_percent from sumcpu)), 2) consumed_cpu_percent, yields FROM v$rsrc_consumer_group where name not like \'ORA$%\''
	return sql_text

def resman_sess_io():
	sql_text = '''
	WITH sessIO as (SELECT current_consumer_group, current_small_read_megabytes+current_large_read_megabytes+current_small_write_megabytes+current_large_write_megabytes sessions_io_total from v$RSRC_SESSION_INFO where current_consumer_group not like \'ORA$%\') 
	select current_consumer_group NAME, sum(current_IO_service_time) IO_SERVICE_TIME, sum(current_small_read_megabytes)  SM_READ_MB, sum(current_large_read_megabytes) LM_READ_MB, 
	sum(current_small_write_megabytes) SM_WRITE_MB, sum(current_large_write_megabytes) LM_WRITE_MB, round(((select SUM(sessions_io_total) 
		from sessIO where current_consumer_group = a.current_consumer_group) / (select SUM(sessions_io_total) from sessIO))*100, 2) TOTAL_IO_PCT, SUM(current_undo_consumption) UNDO_CONSUMPTION FROM v$RSRC_SESSION_INFO a where current_consumer_group not like \'ORA$%\' group by current_consumer_group'''

	return sql_text




def resman_cpu_calculate(con):

	cursor=runStatement(con, resman_perf())
	result1=cursor.fetchall()
	col_names = []
	rows_char = {} 
	for i in range(0, len(cursor.description)):
    		col_names.append(cursor.description[i][0])
	x = PrettyTable(col_names)
	
	for j in result1:
		 x.add_row(j)
		 rows_char[j[0]]=j[7]

	print x
	for y in rows_char:
		value_chart=round(rows_char[y], 1)
		print "{0:30} [{1:5}%]".format(y, str(value_chart)), int(round(value_chart,0))*'#'
	print ''

def resman_sess_io_calculate(con):

	cursor=runStatement(con, resman_sess_io())
	result1=cursor.fetchall()
	col_names = []
	rows_char = {} 
	for i in range(0, len(cursor.description)):
		col_names.append(cursor.description[i][0])

	x = PrettyTable(col_names)
	
	for j in result1:
		x.add_row(j)
		rows_char[j[0]]=j[6]

	print x
	for y in rows_char:
		value_chart=round(rows_char[y], 1)
		print "{0:30} [{1:5} %]".format(y, str(value_chart)), int(round(value_chart,0))*'#'
	print ''



def showMyScreen():
	try:
		options_all = readOptionFile()
		connection_string = readConnectionString()

		print 'I: Connecting to ' + connection_string + ' ...'
		if readRefreshRate() >= 0:
			refresh_rate =  readRefreshRate()
			print 'I: Refresh rate is ' + str(refresh_rate) + ' seconds'
		else:
			print 'E: Refresh rate is invalid. Less than 2 seconds is not allowed'
			sys.exit(1)

		con = connectDB(connection_string)
		while 1:
			# only works for UNIX shells
			time.sleep(refresh_rate)

			os.system('cls' if os.name == 'nt' else 'clear')
			headerText()
			if ('session_io' in options_all) and  ('cpu' in options_all):
				headerCPU(refresh_rate)
				resman_cpu_calculate(con)
				headerSessionIO(refresh_rate)
				resman_sess_io_calculate(con)


	except KeyboardInterrupt:
		print("Ok ok, quitting")
		con.close()
		sys.exit(1)


def main(argv):		
	showMyScreen()		

if __name__ == "__main__":
	main(sys.argv[1:])


#con=connectDB("system/oracle@192.168.56.101/priam")
#f_consumed_cpu_percent(con, 2)
