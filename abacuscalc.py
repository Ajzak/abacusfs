#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import os
import os.path
import redis
import time
import fuzzy_logic
import math
import socket

def main():

	home_dir = os.getenv("HOME")
	arguments = sys.argv
	del arguments[0]
	arguments = ' '.join(arguments)

	#Tell the filesystem that it is going to be a calculation and get the CUID.
	#Afterwards the CUID dir is deleted to avoid possibility that two calculations get the same CUID.
	cuid = os.listdir('abacusfs/newcalc')[0]
	cmd3 = 'rmdir abacusfs/newcalc/*'
	os.system(cmd3)
	
	#Make the folder for in-memory files
	mfiles_exists = os.path.exists('abacusfs/abacusproc/mfiles')
	if not mfiles_exists: 
		os.system('mkdir abacusfs/abacusproc/mfiles')
	
	uv = raw_input('Enter the value of the file: ')
	uv = int(uv)
#	uv = 1
	
	in_mem = 0
	decision_var = fuzzy_logic.logic_init(0, 0, 1, uv)
	
	
	#Extracting the command, input files and output files from argv
	x = arguments.split()
	
	input_files = []
	output_files = []
	implin_files = []
	implout_files = []
	
	command_line = x[0] + ' '
	command_line_pom = command_line
	del x[0]
	
	i = len(x)
	j = 0
	br1 = 0
	br2 = 0
	
	while j < i:
		if x[j] == '-in':
			if len(x[j+1].split('/')) == 1:
				input_files.append(x[j+1])
				command_line = command_line + x[j+1] + ' '
				command_line_pom = command_line_pom + 'abacusfs/' + x[j+1] + ' '
				j = j+1
				br1 = br1+1
				br2 = br2+1
			else:	
				input_files.append(x[j+1])
				command_line = command_line + x[j+1] + ' '
				command_line_pom = command_line_pom + x[j+1] + ' '
				br1 = br1+1
				if 'abacusfs' in x[j+1].split('/'):
					br2 = br2+1				
				j = j+1
		elif x[j] == '-stdin': #Krivo!!!
			input_files.append(x[j+1])
			command_line = command_line + '<' + ' ' + x[j+1] + ' '
			command_line_pom = command_line_pom + '<' + ' ' + x[j+1] + ' '
			j = j + 1
		elif x[j] == '-out':
			output_files.append(x[j+1])
			if decision_var < 50:
				command_line_pom = command_line_pom + 'abacusfs/abacusproc/mfiles' + x[j+1] + ' '
				command_line = command_line + 'abacusproc/mfiles/' + x[j+1] + ' '
				j = j + 1
			if decision_var > 50:	
				command_line_pom = command_line_pom + 'abacusfs/' + x[j+1] + ' '
				command_line = command_line + x[j+1] + ' '
				j = j + 1
		elif x[j] == '-stdout':
			output_files.append(x[j+1])
			
			file_dirs = x[j+1].split('/')[:len(x[j+1].split('/'))-1]
			abacus_dir = ['abacusfs']
			file_dirs_pom = abacus_dir + file_dirs
			file_dirs_pom = '/'.join(file_dirs_pom)
			if not os.path.exists(file_dirs_pom):
				os.makedirs(file_dirs_pom)
			if decision_var < 50:
				abacus_dir = ['abacusfs/abacusproc/mfiles']
				file_dirs = abacus_dir + file_dirs
				file_dirs = '/'.join(file_dirs)
				if not os.path.exists(file_dirs):
					os.makedirs(file_dirs)
				command_line_pom = command_line_pom + '>' + ' ' + 'abacusfs/abacusproc/mfiles/' + x[j+1] + ' '
				command_line = command_line + '>' + ' ' + home_dir + '/AbacusFS/abacusfs/abacusproc/mfiles/' + x[j+1] + ' '
				j = j + 1
			if decision_var >= 50:
				command_line_pom = command_line_pom + '>' + ' ' + home_dir + '/AbacusFS/abacusfs/' + x[j+1] + ' '
				command_line = command_line + '>' + ' ' + x[j+1] + ' '
				j = j + 1
		elif x[j] == '-lit':
			command_line = command_line + x[j+1] + ' '
			command_line_pom = command_line_pom + x[j+1] + ' '
			j = j + 1
		elif x[j] == '-pipe':
			command_line = command_line + '|' + ' '
			command_line_pom = command_line_pom + '|' + ' '
		elif x[j] == '-implin':
			implin_files.append(x[j+1])
			j = j + 1
		elif x[j] == '-implout':
			implout_files.append(x[j+1])
			j = j + 1
		elif x[j] == '-quote':
			if x[j+1] in ['-stdout','-in','-out','-pipe','-stdin']:
				command_line = command_line + '"' + ' '
				command_line_pom = command_line_pom + '"' + ' '
			else:
				command_line = command_line + '"'
				command_line_pom = command_line_pom + '"'
		else:
			if x[j-1] == '-quote':
				command_line = command_line + x[j]
				command_line_pom = command_line_pom + x[j]
			else:
				command_line = command_line + x[j] + ' '
				command_line_pom = command_line_pom + x[j] + ' '
		
		
		j = j + 1
		

	outfiles_list = output_files
	infiles_list = input_files
	
	
	#We start the command and calculate the time it needed for the calculation to run
	start = float(time.time())
	cmd = command_line_pom
	cmd_db = command_line
	os.system(cmd)
	end = float(time.time())
	calc_time = str(end - start)
	
	outfiles = []
	outfiles_cmd = []
	j = len(outfiles_list)
	k = 0
	while k < j:
		outfiles.append(outfiles_list[k])
		if decision_var < 50:
			outfiles_cmd.append('abacusfs/abacusproc/mfiles/'+outfiles_list[k]) #We add 'abacusfs/' to the output file since all the output files are on the abacufs and if the file needs to be in-memory to abacusproc/mfiles.
			k = k + 1
		if decision_var >= 50:
			outfiles_cmd.append('abacusfs/'+outfiles_list[k]) #We add 'abacusfs/' to the output file since all the output files are on the abacufs.
			k = k + 1

	dirs = []
	
	if decision_var < 50:
		in_mem = 1
		outfiles_cmd_pom = outfiles_cmd[0]
		outfiles_cmd_pom = outfiles_cmd_pom.split('/')
		i = outfiles_cmd_pom.index('mfiles')
		while i >= 0:
			del outfiles_cmd_pom[i]
			i = i - 1
		outfiles_cmd_pom = '/'.join(outfiles_cmd_pom)
		
		outfiles_cmd_pom_2 = outfiles_cmd_pom.split('/')
		outfile_file = outfiles_cmd_pom_2[len(outfiles_cmd_pom_2)-1]
		
		cmd_pom = ''
		if len(outfiles_cmd_pom_2) > 1:
			k = len(outfiles_cmd_pom_2)
			j = 0
			while j < k-1:
				dirs.append(outfiles_cmd_pom_2[j])
				cmd_pom = cmd_pom + '../'
				j = j + 1
					
		
		cmd = 'ln -s ' + cmd_pom + 'abacusproc/mfiles/' + outfiles_cmd_pom + ' ' + outfile_file
		dirs = '/'.join(dirs)
		dir_to_chg = 'abacusfs/' + dirs
		os.chdir(dir_to_chg)
		os.system(cmd)
		os.chdir(cmd_pom + '../')
		
	ctime = end
	ctime = float(ctime)
	ctime = str(ctime)
			
#	del infiles_list[0]		
	for infile in infiles_list:
		infiles_list[infiles_list.index(infile)] = infile + ':' + ctime
	in_files_db = ' '.join(infiles_list)
	
	
#	del outfiles_list[0]
	outfiles_list_new = []
	for outfile in outfiles_list:
		outfile = outfile.split('/')
		outfile = outfile[len(outfile) -1]
		outfiles_list_new.append(outfile)
		
	for outfile in outfiles_list_new:
		outfiles_list_new[outfiles_list_new.index(outfile)] = outfile + ':' + ctime
	out_files_db = ' '.join(outfiles_list_new)
	
	
#	for outfiles in outfiles_list:
#		outfiles_list[outfiles_list.index(outfiles)] = outfiles + ':' + ctime
	out_files_db_dirs = ' '.join(outfiles_list)
		
	inno = len(infiles_list)
	outno = len(outfiles_list)
	
	out_files_db = out_files_db_dirs
	
	outfiles_stat = []
	for outfile in outfiles_cmd:
		outfiles_stat.append(os.stat(outfile)[6])

	if outfiles_stat:
		file_size = int(outfiles_stat[0]/1024)
	else:
		file_size = 0
	
	in_mem_out = []
	if decision_var < 50:
		for outfiles_list_i in outfiles_list:
			in_mem_out_pom = home_dir + '/AbacusFS/abacusfs/abacusproc/mfiles/' + outfiles_list_i
			in_mem_out.append(in_mem_out_pom)
			
			outfile_i_pom = outfiles_list_i.split('/')
			outfile_i_pom = outfile_i_pom[len(outfile_i_pom)-1] #file name
			outfiles_i_pom = outfiles_list_i.split('/')[:-1]	
			outfiles_i_pom = 'in_mem_/'+'/'.join(outfiles_i_pom) #dir in the form in_mem_/dir
			
			r = redis.StrictRedis(host = 'localhost', port = 6379, db = 0)
			r.sadd(outfiles_i_pom,outfile_i_pom) #add file in the directory 
			
		
	in_mem_out = ' '.join(in_mem_out)	
	
	node_name = socket.gethostname()	
	
	if br1 <= br2:
		on_abacus = 1
	else:
		on_abacus = 0
	
	
#	print os.getcwd()
	f = open('abacusfs/abacusproc/'+cuid, 'w+')
	f.write(str(inno)+'\n'+in_files_db+'\n'+str(outno)+'\n'+out_files_db+'\n'+cmd_db+'\n'+calc_time+'\n'+str(uv)+'\n'+str(file_size)+'\n'+str(in_mem)+'\n'+in_mem_out+'\n'+node_name+'\n'+str(on_abacus)+'\n')
	f.close()
	
	f = open('abacusfs/abacusproc/status', 'a')
	f.write(cuid+' '+'0'+'\n')
	f.close()
			
	cmd1 = 'mkdir abacusfs/status'
	os.system(cmd1)
	cmd2 = 'ls abacusfs/status'
	os.system(cmd2)
	cmd3 = 'rmdir abacusfs/status'
	os.system(cmd3)
	
if __name__ == '__main__':
    main()
   
