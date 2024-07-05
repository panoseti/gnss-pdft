from argparse import ArgumentParser
from datetime import datetime
import sys 
sys.path.append('../')
import numpy as np
import os
import pandas as pd
import pypff
import pytz
import time


# Takes pff file and get a timestamp - Does not work if there is more than one quabo (7/5/24)

# Stealing from Wei's wr_to_unix code in util/pff.py


def wr_to_unix_extra(pkt_tai, tv_sec, tv_sec_long, globalCounter):
	d = (tv_sec - pkt_tai + 37)%1024
	if d == 0:
		return 0 #tv_sec
	elif d == 1:
		return -1 #tv_sec - 1
	elif d == 1023:
		return 1 #tv_sec + 1
	else:
		print('pkttai is ' + str(pkt_tai))
		print('tv_sec is ' + str(tv_sec))
		print('extended time is ' + str(tv_sec_long))
		time_utc = datetime.utcfromtimestamp(tv_sec_long)
		times_pt = time_utc.astimezone(pytz.timezone('US/Pacific'))
		print('extended time written as a PT time: ' + str(times_pt.strftime("%Y-%m-%d %H:%M:%S")))
		print('global counter is ' + str(globalCounter))
		#return 0
		raise Exception('WR and Unix times differ by > 1 sec: pkt_tai %d tv_sec %d d %d'%(pkt_tai, tv_sec, d))




def wr_to_unix(pkt_tai, tv_sec):
	d = (tv_sec - pkt_tai + 37)%1024
	if d == 0:
		return 0 #tv_sec
	elif d == 1:
		return -1 #tv_sec - 1
	elif d == 1023:
		return 1 #tv_sec + 1
	else:
		print('pkttai is ' + str(pkt_tai))
		print('tv_sec is ' + str(tv_sec))
		#return 0
		raise Exception('WR and Unix times differ by > 1 sec: pkt_tai %d tv_sec %d d %d'%(pkt_tai, tv_sec, d))


# Some arguments - takes in a directory + a savefile
# Example of calling the script: python pypff_getmetada.py -d directoryName -sf name.csv file1 file2 file3
parser = ArgumentParser()
parser.add_argument("-d", "--dir", type=str, default='./', help="Give a directory look for files defaults to cwd", required = False)
parser.add_argument("-v", "--verbose", action = 'store_true', help="Verbose mode")
parser.add_argument("-sf", "--save", type=str, default='saveFile.csv', help="Savefile default is saveFile.csv if none given - will overwrite", required = False)


# Collect files
if __name__ == '__main__':
	args, unknownargs = parser.parse_known_args()
	allFiles = []
	if not unknownargs:
		print('Globbing all pff files in ' + args.dir)
		for aFile in os.listdir(args.dir):
			if aFile.endswith(".pff"):
				if 'img16' in aFile or 'img8' in aFile or 'ph256' in aFile or 'ph1024' in aFile:
					allFiles.append(os.path.join(args.dir, aFile))
		if not allFiles:
			print('No files to analyze')
			sys.exit(1)
		print(allFiles)
	else:
		for aFile in unknownargs:
			if not(os.path.isfile(os.path.join(args.dir, aFile))):
				print('Could not find file ' + str(aFile))
				sys.exit(1)
			allFiles.append(os.path.join(args.dir, aFile))

# Get second and nanosecond information
allTimesSec = []
allTimesNanos= []
for aFile in allFiles:
	print('ON FILE: ' + str(aFile))
	dpff = pypff.io.datapff(aFile)
	dpff_d, dpff_md = dpff.readpff(ver='qfb', metadata=True)

	# Check if the key is in the file
	if not('tv_sec' in dpff_md) or not('pkt_nsec' in dpff_md):
		print('Key not found in ' + str(aFile))
		print('Not including it in the dataset')
	else:
		if args.verbose:
			print('Getting seconds data from Unix')
		holderTimes_sec = [x for x in dpff_md['tv_sec'].ravel()] #,
		if args.verbose:
			print('Getting 10-bit seconds data from White Rabbit')
		holderTimes_sec_wr = [x[0] for x in dpff_md['pkt_tai']]
		if args.verbose:
			print('Calculating delta between Unix seconds and White rabbits seconds')
		print('length of white rabbit ' + str(len(holderTimes_sec_wr)))
		print('length of other timestamp ' + str(len(holderTimes_sec)))
		badIndex = 283108	
		print('white rabbit timestamp: ' + str(holderTimes_sec_wr[badIndex-5:badIndex+6]))
		print('other timestamp ' + str([x%1024 for x in holderTimes_sec[badIndex-5:badIndex+6]]))
		print('delta ' + str(np.asarray(holderTimes_sec_wr[badIndex-5:badIndex+6])) - np.asarray([x%1024 for x in holderTimes_sec[badIndex-5:badIndex+6]]))
		deltas = []
		for aDelta in range(len(holderTimes_sec_wr)):
			#print('ON ROUND '  + str(aDelta))
			deltas.append(wr_to_unix_extra(holderTimes_sec_wr[aDelta], int(holderTimes_sec[aDelta]%1024), holderTimes_sec[aDelta], aDelta))
		#deltas = [wr_to_unix_extra(val[0], int(val[1]%1024), val[1]) for val in zip(holderTimes_sec_wr, holderTimes_sec)]
		if args.verbose:
			print('Converting UTC time to PT time')
		holderTimes_sec = np.asarray(holderTimes_sec) + np.asarray(deltas)
		holderTimes_utc = [datetime.utcfromtimestamp(x) for x in holderTimes_sec]
		holderTimes_pt = [x.astimezone(pytz.timezone('US/Pacific')) for x in holderTimes_utc]
		holderNs = [x for x in dpff_md['pkt_nsec'].ravel()]
		if args.verbose:
			print('Formatting time string + nanosecond string')
		for aTime, aNano in zip(holderTimes_pt, holderNs):
			allTimesSec.append(aTime.strftime("%Y-%m-%d %H:%M:%S"))
			allTimesNanos.append(aNano)

# Create a csv from the dataframe
if args.verbose:
	print('Creating a dataframe using collected data')
allData = [[val[0], val[1]] for val in zip(allTimesSec, allTimesNanos)]
df = pd.DataFrame(allData, columns = ['Time (PT)', 'Extra nanoseconds (ns)'])

if args.verbose:
	print('Saving file to ' + str(args.save))
# Save the data to disk 
with open(args.save, 'w') as f:
	df.to_csv(args.save)

