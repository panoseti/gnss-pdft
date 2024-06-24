from time import sleep
import queue
import pygnssutils as pygnss
import matplotlib.pyplot as plt 
import numpy as np 
import sys


# Simple code to create a NTRIP client using pygnssutils library 

#gnssntripclient --server 132.239.152.175 --port 2103 --ggamode 1 --reflat 37.76 --reflon -122.46 --ntripversion 1.0 --ntripuser CRTNBERKBENG --ntrippassword BERKBENGSURV --datatype RTCM --mountpoint UCSF_RTCM3

# Obtained from http://132.239.152.175:2103/

# Name of the server
serverName = '132.239.152.175'

# Port numbers
portNumber = 2103

# Flag if you are using https or http
https = 0

# Type of data coming from the GNSS station
datatype = 'RTCM'

# Name of the receiver 
mountpoint = 'UCSF_RTCM3'

# From the documentation: To retrieve correction data from a designated mountpoint 
# (this will send NMEA GGA position sentences to the server at intervals of 60 seconds, 
# based on the supplied reference lat/lon):
ggamode = 0

# Latitude of Campbell Hall (found quickly on Google)
latitude = '37.8731'
longitude = '-122.2571'


# NTRIP Version (I had to fiddle with this until I got data)
ntripversion = '1.0'

# My username / password
username = 'CRTNBERKBENG'
userpass = 'BERKBENGSURV'

# Silly way to set settings but it works I guess
settingsDict = {}
settingsDict['server'] = serverName 
settingsDict['port'] = portNumber 
settingsDict['reflat'] = latitude
settingsDict['reflon'] = longitude
settingsDict['ggamode'] = ggamode 
settingsDict['mountpoint'] = mountpoint 
settingsDict['version'] = ntripversion
settingsDict['ntripuser'] = username 
settingsDict['ntrippassword'] = userpass
settingsDict['https'] = https
settingsDict['datatype'] = datatype

# If you leave the mountpoint blank, you will get the nearest source point to the reference latitude and longitude
# Sphinx documentation: https://www.semuconsulting.com/pygnssutils/pygnssutils.html#module-pygnssutils.gnssntripclient
server = pygnss.gnssntripclient.GNSSNTRIPClient(None)
data = queue.Queue()


# Collect data and store it in a queue
try:
	with server as gnc:
		streaming = gnc.run(server = settingsDict['server'], port = settingsDict['port'], \
		   reflat = settingsDict['reflat'], reflon = settingsDict['reflon'], \
		   ggamode = settingsDict['ggamode'], mountpoint = settingsDict['mountpoint'], \
		   version = settingsDict['version'], ntripuser = settingsDict['ntripuser'], \
		   ntrippassword = settingsDict['ntrippassword'], https = settingsDict['https'],\
		   datatype = settingsDict['datatype'], output = data)
		while streaming:  # run until user presses CTRL-C
			sleep(1)
		sleep(1)
except KeyboardInterrupt:
	pass


