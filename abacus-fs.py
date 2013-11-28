#!/usr/bin/env python

import os
import sys
import errno
import stat
import fcntl
import redis
import time
import os.path
import uuid
import fuzzy_logic
import collections
import socket

import fuse
from fuse import Fuse


if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-python doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

# Since we are using a separate class for files (XmpFile) we need to tell FUSE that with this line of code.
fuse.feature_assert('stateful_files', 'has_init')

#This part of the code parses python flags to file system modes (e.g. os.O_RDONLY to 'r' for read only mode).
def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m


#This is the main FUSE class.
class Xmp(Fuse):

    def __init__(self, *args, **kw):
		
		Fuse.__init__(self, *args, **kw)
		
		print 'Starting the FUSE based file system'
		print '\n'
        
		self.root = '/' #if no directory is given the file system is mounted at this directory (now root - can be set to any other).
		self.file_class = self.XmpFile #We define the name of the file class since we are using a seperate class.
		
		global home_dir
		home_dir = os.getenv("HOME")
		
		#Connect to the database
		self.redis_host = 'localhost'
		global r
		r = redis.StrictRedis(host = self.redis_host, port = 6379, db = 0)
		
		all_dirs = [x[0] for x in os.walk(home_dir + '/AbacusFS/test')]
		
		for directory in all_dirs:
			directory_pom = directory.split('/')			
			if not 'newcalc' in directory_pom and not '.Trash-1001' in directory_pom:
				directory_pom = directory_pom[6:]
				directory_pom = '/'.join(directory_pom)
				r.delete('in_mem_/'+directory_pom)
		
    def getattr(self, path):
		
		path_exists = os.path.exists("." + path)
		
		if not path_exists:
			#We get a path that looks like: "/dir1/dir2/../file_name" and in the database we dont have a relative path so we need to transform it to "dir1/dir2/.../file_name"
			path_pom = path.split('/')
			file_name = path_pom[len(path_pom)-1]
			del path_pom[len(path_pom)-1]
			file_name = file_name.split('md_')		

			if len(file_name) == 2:			
				path_pom = '/'.join(path_pom)
				path_pom1 = path_pom
				path_pom = path_pom + '/' + file_name[1]
				path_pom = path_pom[1:]
								
				cuid = r.get(path_pom)
				
				md_all = r.hgetall(cuid)
				md_all_string = ('Number of times the calculation was started: '+md_all['count']+'\n'+
									'User value: '+md_all['uv']+'\n'+
									'Number of times the file was accessed: '+md_all['foa']+'\n'+
									'Time needed for the calculation to execute: '+md_all['calc_time']+'\n'+
									'Input files: '+md_all['in']+'\n'
									'Output files: '+md_all['out']+'\n'
									'Command line: '+md_all['cmd']+'\n')

				path_new_md = path_pom1 + '/' + 'md_'+file_name[1]		
				
				f = open(os.getcwd() + path_new_md,'w')
				f.write(md_all_string)
				os.system('more '+os.getcwd() + path_new_md)
				f.close()
				
				
			old = False
			#We chech whether the file exists in the db == is the result of some calculation	
			cuid = r.get(file_name)
			
			if(cuid):
				cmd = r.hget(cuid, 'cmd')
				
				is_in_mem = r.hget(cuid, 'in_mem')
				is_in_mem = int(is_in_mem)
				
				if is_in_mem:
					old = False
					#Get the input and output files from the DB
					infiles = r.hget(cuid,'in')
					infiles = infiles.split()
				
					outfiles = r.hget(cuid,'in_mem_out')
					outfiles = outfiles.split()
				
					#Extract input files and get the ctimes of the input files.	Sort the dates.
					files_in = []
					dates_in = []
					for inf in infiles:
						files_in.append(inf.split(':')[0])
				
					for file_in in files_in:
						#file_in = file_in.split('/')[1]
						dates_in.append(os.stat(file_in)[9])
						
					dates_in.sort()
					newest_date = dates_in[len(dates_in)-1]
				
					#Extract output files and get the ctimes of the output files.
					files_out = []
					dates_out = []
					for outf in outfiles:
						files_out.append(outf.split(':')[0])
				
					for file_out in files_out:
						#file_out = file_out.split('/')[1]
						dates_out.append(os.stat(file_out)[9])
					
					#See whether any output file is out-of-date.
					for date in dates_out:
						if (int(date) < int(newest_date)):
							old = True

					#If the output file is out-of-date we need to re-calculate it and then read it.
					if old:
						cmd = r.hget(cuid, 'cmd')
						
						os.system(cmd)
					
						ctime = time.time()
						ctime = int(ctime)
						ctime = str(ctime)
					
						#Update the count number
						count_old = r.hget(cuid, 'count')
						count_old = int(count_old)
						count = count_old + 1
						count_new = str(count)
						r.hset(cuid, 'count', count_new)
					
					
						file_size = float(os.stat(file_out)[6])
						file_size = file_size/1024
						file_size = int(file_size)
						file_size = str(file_size)
						r.hset(cuid, 'fsize', file_size)
					
						freq_of_acc_old = r.hget(cuid, 'foa')
						freq_of_acc_old = int(freq_of_acc_old)
						freq_of_acc = freq_of_acc_old + 1
						freq_of_acc_new = str(freq_of_acc)
						r.hset(cuid, 'foa', freq_of_acc_new)
						r.hset(cuid, 'rel_f', '1')
						
						
					
				#If the output file is up-to-date just read it.	
				elif not old:
					return os.lstat("." + path)
				
				else:
					os.system(cmd)
					
					ctime = time.time()
					ctime = int(ctime)
					ctime = str(ctime)
					
					#Update the count number
					count_old = r.hget(cuid, 'count')
					count_old = int(count_old)
					count = count_old + 1
					count_new = str(count)
					r.hset(cuid, 'count', count_new)
					
					freq_of_acc_old = r.hget(cuid, 'foa')
					freq_of_acc_old = int(freq_of_acc_old)
					freq_of_acc = freq_of_acc_old + 1
					freq_of_acc_new = str(freq_of_acc)
					r.hset(cuid, 'foa', freq_of_acc_new)
					r.hset(cuid, 'rel_f', '1')
#					return os.lstat("." + path)
				
				if is_in_mem:
					outfiles = r.hget(cuid,'in_mem_out')
					outfiles = outfiles.split()
					
					#Extract output files and get the ctimes of the output files.
					files_out = []
					dates_out = []
					for outf in outfiles:
						files_out.append(outf.split(':')[0])
					
					freq_of_acc = r.hget(cuid, 'foa')
					freq_of_acc = int(freq_of_acc)
					file_size = float(os.stat(file_out)[6])
					file_size = file_size/1024
					file_size = int(file_size)
					count = r.hget(cuid,'count')
					count = int(count)
					user_value = r.hget(cuid, 'uv')
					user_value = int(user_value)
					
					
					
					print '\n'
					print freq_of_acc
					print file_size
					print count
					print user_value
					print '\n'
					
					
					decision_var = fuzzy_logic.logic_init(freq_of_acc, file_size, count, user_value)
					
					if decision_var >= 50:
						outfile = r.hget(cuid,'out')
						cmd1 = 'rm -rf ' + outfile #delete symlink
						cmd2 = 'cp ' + home_dir + '/AbacusFS/abacusfs/abacusproc/mfiles/' + outfile + ' ' + outfile #Copy file from memory to abacusfs
						cmd3 = 'rm -rf ' + home_dir + '/AbacusFS/abacusfs/abacusproc/mfiles/' + dirs  #cmd = 'rm -rf ../../abacusproc/mfiles/dir1/dir2/file' - delete the file from memory
						os.system(cmd1)
						os.system(cmd2)	
						os.system(cmd3)
				#else:
					#return os.lstat("." + path)
		#else:
		return os.lstat("." + path)

    def readlink(self, path):
        return os.readlink("." + path)

    def readdir(self, path, offset):
		path_pom = path.split('/')
			
		#Help function that makes a tree of directories if they dont exist.
		def mkdir_p(path):
			try:
				os.makedirs(path)
			except OSError as exc:
				if exc.errno == errno.EEXIST:
					pass
				else: raise
		
		#When we do "ls status" we want the file system to check the status file in proc file system and transfer the information to the DB if needed.
		if path_pom[1] == 'status':
			print 'Status se cita'
			print '\n'
			#We read the cuids of the calculations from the status file and the statuses of the calculations (are the information in the db or not).
			cuids = open(home_dir + '/AbacusFS/abacusfs/abacusproc/status','r').read().splitlines()
			#cuids = ['cuid1 0', 'cuid2 1', ....]
			for cuid in cuids:
				cuid_pom = cuid.split(' ')
				cuid_id = cuid_pom[0]
				cuid_status = cuid_pom[1]
				
				
				if cuid_status == '0':
					index = cuids.index(cuid) #Index of the cuid with the status 0. 
					calc_data = open(home_dir + '/AbacusFS/abacusfs/abacusproc/'+cuid_id,'r').read().splitlines()
				
					outfiles = calc_data[3].split(' ')
					outf = []
					for outfile in outfiles:
						outf.append(outfile.split(':')[0])
					
					for out_file in outf:
						r.set(out_file,cuid_id)						
					
					r.hset(cuid_id, 'inno', calc_data[0])
					r.hset(cuid_id, 'in', calc_data[1])
					r.hset(cuid_id, 'outno', calc_data[2])
					r.hset(cuid_id, 'out', calc_data[3])
					r.hset(cuid_id, 'cmd', calc_data[4])
					r.hset(cuid_id, 'calc_time', calc_data[5])
					r.hset(cuid_id, 'uv', calc_data[6])
					r.hset(cuid_id, 'fsize', calc_data[7])
					r.hset(cuid_id, 'in_mem', calc_data[8])
					r.hset(cuid_id, 'in_mem_out', calc_data[9])
					r.hset(cuid_id, 'count', '1')
					r.hset(cuid_id, 'foa', '1')
					r.hset(cuid_id, 'rel_f', '0')
					
					cuid_new = cuid_id + ' ' + '1'
					cuids[index] = cuid_new
					
			for cuid in cuids:
				index = cuids.index(cuid)
				cuids[index] = cuids[index]+'\n'
			
			f = open(home_dir + '/AbacusFS/abacusfs/abacusproc/status','w')
			f.seek(0)
			f.writelines(cuids)
			f.close()
					
		if path_pom[1] == 'newcalc':
			print 'Usao u skriptu'
			cuid = uuid.uuid4()
			cuid = str(cuid)
			dirs_to_make = 'newcalc' + '/' + cuid
			mkdir_p(dirs_to_make)
			
		#We have in memory files in db stored in in_mem_path and we take these files into account also when doing "ls" 
		path_in_mem = 'in_mem_'+path
		dirs =list(r.smembers(path_in_mem))
		
		for e in os.listdir("." + path):
			dirs.append(e)
			
		dirs_pom = collections.Counter(dirs)
		dirs = list(dirs_pom)		
			
		for diri in dirs:
			yield fuse.Direntry(diri)

    def unlink(self, path):
		path_pom = path.split('/')
		
		#Path je: '/dir1/dir2/file', path_pom je: [' ','dir1','dir2','file']
		file_name = path_pom[len(path_pom) - 1]
		del path_pom[0] #path_pom = ['dir1','dir2','file']
		dirs = '/'.join(path_pom) #dirs = 'dir1/dir2/file'
		file_in_mem = path_pom[len(path_pom)-1]
		del path_pom[len(path_pom)-1]
		path_in_mem = '/'.join(path_pom) #path_in_mem = 'dir1/dir2'
		
		is_in_mem = 0
		#Get the cuid of the file from the database and see whether the file is in-memory file.
		cuid = r.get(dirs)
		if (cuid):
			print 'Usao u cuid'
			is_in_mem = r.hget(cuid, 'in_mem')
			if is_in_mem:
				is_in_mem = int(is_in_mem)
		
		if is_in_mem:
			#If the symlink is deleted so must be the real file that is in memory.
			cmd = 'rm -rf ' + home_dir + '/AbacusFS/abacusfs/abacusproc/mfiles/' + dirs  #cmd = 'rm -rf ../../abacusproc/mfiles/dir1/dir2/file'
			os.system(cmd)
			
			path_in_mem = 'in_mem_/ '+path_in_mem

			#Delete the file from the database.
			r.srem(path_in_mem,file_in_mem)
		os.unlink("." + path)


    def rmdir(self, path):
        os.rmdir("." + path)

    def symlink(self, path, path1):
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
		try:
			os.makedirs("." + path, mode)
		except OSError as exception:
			if exception.errno != errno.EEXIST:
				raise
#		os.mkdir("." + path, mode)

    def utime(self, path, times):
        os.utime("." + path, times)

	def access(self, path, mode):
		print '\n'
		print '\n'
		print 'Path je:', path
		print '\n'
		print 'mode je:', mode
		print '\n'
		print '\n'
        if not os.access("." + path, mode):
            return -errno.EACCES

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        return os.statvfs(".")

    def fsinit(self):
        os.chdir(self.root)

    class XmpFile(object):

        def __init__(self, path, flags, *mode):

			novi_path = home_dir + '/AbacusFS/mem/testni-fajl'
			if path == '/testni-fajl':
				self.file = os.fdopen(os.open(novi_path, flags, *mode),
								flag2mode(flags))
			else:					
				self.file = os.fdopen(os.open("." + path, flags, *mode),
								flag2mode(flags))
								
			self.fd = self.file.fileno()
			self.path = path

        def read(self, length, offset):
			
			#We get a path that looks like: "/dir1/dir2/../file_name" and in the database we dont have a relative path so we need to transform it to "dir1/dir2/.../file_name"
			file_name = self.path
			file_name = file_name.split('/')
			del file_name[0]
			file_name = '/'.join(file_name)
			
			#The old flag tels us whether the file is out-of or up-to date.
			old = False
			
			#We chech whether the file exists in the db == is the result of some calculation	
			cuid = r.get(file_name)
			if (cuid):
				#Get the input and output files from the DB
				infiles = r.hget(cuid,'in')
				infiles = infiles.split()
				
				outfiles = r.hget(cuid,'out')
				outfiles = outfiles.split()
				
				#Extract input files and get the ctimes of the input files.	Sort the dates.
				files_in = []
				dates_in = []
				for inf in infiles:
					files_in.append(inf.split(':')[0])
				
				for file_in in files_in:
					#file_in = file_in.split('/')[1]
					dates_in.append(os.stat(file_in)[9])
						
				dates_in.sort()
				if dates_in:
					newest_date = dates_in[len(dates_in)-1]
				else:
					newest_date = 0
				
				#Extract input files and get the ctimes of the input files.
				files_out = []
				dates_out = []
				for outf in outfiles:
					files_out.append(outf.split(':')[0])
				
				for file_out in files_out:
					#file_out = file_out.split('/')[1]
					dates_out.append(os.stat(file_out)[9])
					
				#See whether any output file is out-of-date.
				for date in dates_out:
					if (int(date) < int(newest_date)):
						old = True

				#If the output file is out-of-date we need to re-calculate it and then read it.
				if old:
					cmd = r.hget(cuid, 'cmd')
					os.system(cmd)
					
					ctime = time.time()
					ctime = int(ctime)
					ctime = str(ctime)
					
					#Update the count number
					count_old = r.hget(cuid, 'count')
					count_old = int(count_old)
					count = count_old + 1
					count_new = str(count)
					r.hset(cuid, 'count', count_new)
					
					
					file_size = float(length)
					file_size = file_size/1024
					file_size = int(file_size)
					file_size = str(file_size)
					r.hset(cuid, 'fsize', file_size)
					
					freq_of_acc_old = r.hget(cuid, 'foa')
					freq_of_acc_old = int(freq_of_acc_old)
					freq_of_acc = freq_of_acc_old + 1
					freq_of_acc_new = str(freq_of_acc)
					r.hset(cuid, 'foa', freq_of_acc_new)
					r.hset(cuid, 'rel_f', '1')
					
					
					self.file.seek(offset)
					return self.file.read(length)
					
				#If the output file is up-to-date just read it.	
				elif not old:
					self.file.seek(offset)
					return self.file.read(length)
				
				#If the file does not exist or there is some problem.
				else:
					return -errno.ENOENT
			else:				
				self.file.seek(offset)
				return self.file.read(length)		
					
        def write(self, buf, offset):
			file_name = self.path
			file_name = file_name.split('/')
			del file_name[0]
			file_name = '/'.join(file_name)
			
			file_name_pom = file_name.split('uv_')
			
			print '\n'
			print file_name
			print len(file_name_pom)
			print buf.split('\n')
			print '\n'
			
			if len(file_name_pom) == 2:
				file_name_uv = file_name_pom[1]
			
				#We chech whether the file exists in the db == is the result of some calculation	
				cuid = r.get(file_name_uv)
		
				if(cuid):
					uv = buf.split('\n')[0]
					r.hset(cuid, 'uv', uv)
					
				return len(buf)
				
				
			else:
				self.file.seek(offset)
				self.file.write(buf)
				return len(buf)
			
			
        def release(self, flags):
			path = self.path
			path_pom = path[1:]
			path_pom = path_pom.split('/')
			file_name = path_pom[len(path_pom)-1]
			path_pom = '/'.join(path_pom)

			file_name_pom = file_name.split('uv_')
			
			if len(file_name_pom) == 2:
				cmd = 'rm -rf '+path_pom
				os.system(cmd)
				
			file_name_pom2 = file_name.split('md_')
			
			if len(file_name_pom2) == 2:
				cmd = 'rm -rf '+path_pom
				os.system(cmd)
			
			
			#Path je: '/dir1/dir2/file', path_pom je: [' ','dir1','dir2','file']
			path_pom = self.path.split('/')
			del path_pom[0] #path_pom = ['dir1','dir2','file']
			dirs = '/'.join(path_pom) #dirs = 'dir1/dir2/file'
			file_in_mem = path_pom[len(path_pom)-1]
			del path_pom[len(path_pom)-1]
			path_in_mem = '/'.join(path_pom) #path_in_mem = 'dir1/dir2'
			path_in_mem = 'in_mem_/ '+path_in_mem
			
			#We check whether the file exists in the db == is the result of some calculation	
			cuid = r.get(file_name)
			if (cuid):
				release_flag = r.hget(cuid, 'rel_f')
				release_flag = int(release_flag)
				if release_flag == 1:
					user_value = r.hget(cuid, 'uv')
					user_value = int(user_value)
					file_size = r.hget(cuid, 'fsize')
					file_size = int(file_size)
					count = r.hget(cuid, 'count')
					count = int(count)
					freq_of_acc = r.hget(cuid, 'foa')
					freq_of_acc = int(freq_of_acc)
					
					decision_var = fuzzy_logic.logic_init(freq_of_acc, file_size, count, user_value)
					
					print decision_var
					
					if decision_var < 50:
						#print 'Radi ovaj dio'
						del_cmd = 'rm -rf ' + file_name
						#print '\n'
						#print del_cmd
						#print '\n'
						os.system(del_cmd)
						
						#If the file was not in_memory we need to change it
						if not r.sismember(path_in_mem, file_in_mem):
							r.sadd(path_in_mem, file_in_mem)
						#print '\n'
						print 'SMALL!'
						self.file.close()
				
				#	if decision_var >= 45 and decision_var < 50:
				#		print '\n'
				#		print 'SREDNJE'
				#		self.file.close()
						
					if decision_var >= 50:
						#if the file was in memory we need to remove it
						if r.sismember(path_in_mem, file_in_mem):
							r.srem(path_in_mem, file_in_mem)
							
						print '\n'
						print 'BIG'
						self.file.close()
					r.hset(cuid,'rel_f','1')
						
				else:
					self.file.close()
			else:
				self.file.close()

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            self._fflush()
            # cf. xmp_flush() in fusexmp_fh.c
            os.close(os.dup(self.fd))

        def fgetattr(self):
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.file.truncate(len)

        def lock(self, cmd, owner, **kw):
            # The code here is much rather just a demonstration of the locking
            # API than something which actually was seen to be useful.

            # Advisory file locking is pretty messy in Unix, and the Python
            # interface to this doesn't make it better.
            # We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
            # way. The following implementation *might* work under Linux. 
            #
            # if cmd == fcntl.F_GETLK:
            #     import struct
            # 
            #     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
            #                            kw['l_start'], kw['l_len'], kw['l_pid'])
            #     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
            #     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
            #     uld2 = struct.unpack('hhQQi', ld2)
            #     res = {}
            #     for i in xrange(len(uld2)):
            #          res[flockfields[i]] = uld2[i]
            #  
            #     return fuse.Flock(**res)

            # Convert fcntl-ish lock parameters to Python's weird
            # lockf(3)/flock(2) medley locking API...
            op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
                   fcntl.F_RDLCK : fcntl.LOCK_SH,
                   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
            if cmd == fcntl.F_GETLK:
                return -EOPNOTSUPP
            elif cmd == fcntl.F_SETLK:
                if op != fcntl.LOCK_UN:
                    op |= fcntl.LOCK_NB
            elif cmd == fcntl.F_SETLKW:
                pass
            else:
                return -EINVAL

            fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])



def main():

    usage = """
AbacusFS file system!

""" + Fuse.fusage

    server = Xmp(version="%prog " + fuse.__version__,
                 usage=usage)

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    server.parse(values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print >> sys.stderr, "can't enter root of underlying filesystem"
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main()
