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


# Takes pff file and get a timestamp

# Some arguments - takes in a directory + a savefile
# Example of calling the script: python pypff_getmetada.py -d directoryName -sf name.csv file1 file2 file3
parser = ArgumentParser()
parser.add_argument("-d", "--dir", type=str, default='./', help="Give a directory look for files defaults to cwd", required = False)
parser.add_argument("-sf", "--save", type=str, default='saveFile.csv', help="Savefile", required = False)


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
		holderTimes_sec = [x for x in dpff_md['tv_sec'].ravel()] #, 
		print(holderTimes_sec[:10])
		holderTimes_utc = [datetime.utcfromtimestamp(x) for x in holderTimes_sec]
		holderTimes_pt = [x.astimezone(pytz.timezone('US/Pacific')) for x in holderTimes_utc]
		holderNs = [x for x in dpff_md['pkt_nsec'].ravel()]
		for aTime, aNano in zip(holderTimes_pt, holderNs):
			allTimesSec.append(aTime.strftime("%Y-%m-%d %H:%M:%S"))
			allTimesNanos.append(aNano)

# Create a csv from the dataframe
allData = [[val[0], val[1]] for val in zip(allTimesSec, allTimesNanos)]
df = pd.DataFrame(allData, columns = ['Time (PT)', 'Extra nanoseconds (ns)'])

# Save the data to disk 
with open(args.save, 'w') as f:
	df.to_csv(args.save)

