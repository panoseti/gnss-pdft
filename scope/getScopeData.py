# Various libraries
from datetime import datetime
import numpy as np 
import matplotlib.pyplot as plt 
import pyvisa as visa 
import sys
import time 

# Simple code to collect data from scope. Right now the only way to stop it is to CTRL+C the program

# Hardocded IP address of the scope
instName = 'TCPIP::192.168.2.249::INSTR'

# Create a pyvsia instance with a timeout of 5 seconds
try:
	# Print all the devices connected to your computer
	# print(rm.list_resources()) 
	inst = visa.ResourceManager().open_resource(instName)
	# Get the identity of the connected device
	# print(inst.query(*IDN?)) 
	inst.timeout = 5000 
except Exception as e:
	print('COULD NOT CONNECT TO DEVICE EXITING NOW...')
	sys.exit()

measCommand = 'MEASU:MEAS1:RESU:CURR:MEAN?' # Measurement command from the manual



# CONFIGURATION SETTINGS (SHOULD ULTIMATELY BE MOVED TO COMMAND LINE ARGUMENTS)
timeInterval = 1 # Measure data every second (excessive)
writeDir = './ScopeData/' # Name of the save directory - NOTE: This WILL fail if you don't create this directory
saveFileBase = writeDir + 'scopeData' # Name of the save file - NOTE: This WILL overwrite things
saveFileCounter = 1
fileWriteInterval = 360 # in seconds
totalFiles = 500 # Total number of files


# 1 index instead of 0 index
for fileCounter in range(1, totalFiles+1):
	saveFileName = saveFileBase + '_' + str(fileCounter) + '.txt' #
	print('WRITING TO...\t' + saveFileName)
	
	# Give a header
	with open(saveFileName, 'w') as aFile:
		aFile.write('TIME (PT) \t\t\t DELTA (ns)\n')
	
	# Start measuring data
	with open(saveFileName, 'a') as aFile:
		startTime = datetime.now()
		while((datetime.now() - startTime).total_seconds() < fileWriteInterval):
			# Have the except block to deal with possible timeouts
			try:
				currentTime = datetime.now() # current date and time
				currentTimeString = currentTime.strftime("%m/%d/%Y %H:%M:%S.%f:" ) # Write the date to a file
				separation = inst.query(measCommand)
				endCommandTime = datetime.now()
				measuredSeparation = float(separation.strip())*1e9 # Save things in nanoseconds
				# Makes it so you don't have excessive precision
				aFile.write(currentTimeString + '\t' + str(round(measuredSeparation, 3)) + '\n')
			except Exception as e:
				print(e)
				# A dumb error message
				print("DARN TOOTIN' YOU PROBABLY HAD A TIMEOUT")
			time.sleep(timeInterval - 0.001 - (endCommandTime - currentTime).total_seconds())

print('ENDED')

