#The MIT License (MIT)
#
#Copyright (c) 2020 Peterfdej
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

# Record_ok.py is a simple Python script for recording live ok.ru of users stored in a csv file.
# Put record_ok.py and the cvs file in the same directory. Recordings will also be stored in that directory.
# Advice: max 10 users in csv.
# You can run record-ok.py multiple times, when you create multiple directories, each with his own
# record_ok.py and csv file.
# It is possible te edit the csv file while record_ok.py is running.
# Use Notepad++ for editing.
# Format csv: 123456789012,210987654321
#
# Requirements:	- Python 3
#				- ffmpeg
#
# Usage: 	python record_ok.py (non converting to mp4)
#			python record_ok.py -c (Recordings will be converted to mp4 after ending broadcast)
from bs4 import BeautifulSoup
import sys, time, os, getopt, csv
import os.path
import subprocess
import json
import urllib.request, urllib.error

OK_URL = 'https://ok.ru/profile/'
OK_URL_END = '/video?st._aid=NavMenu_User_Friend_Video'
BROADCAST_URL = 'https://ok.ru/live/'

broadcastdict = {}
p = {}
p1 = {}
convertmp4 = 0

args = sys.argv[1:]
if len(args):
	CW = args[0]
	if CW == '-c':
		convertmp4 = 1
		print ("Recordings will be converted to mp4 after ending broadcast.")

if os.name == 'nt':
	FFMPEG = 'ffmpeg.exe'
else:
	FFMPEG = 'ffmpeg'
	
def file_size(fname):
        statinfo = os.stat(fname)
        return statinfo.st_size
	
def get_live_broadcast(user):
	req = urllib.request.Request(OK_URL + user + OK_URL_END)
	broadcastid = ""
	try:
		response = urllib.request.urlopen(req)
		r = response.read()
		soup = BeautifulSoup(r, 'html.parser')
		page_container = soup.find(id='listBlockPanelFriendVideoLiveRBlock')
		try:
			if len(page_container) > 1:
				data_string = str(page_container)
				broadcastid = data_string[data_string.find('data-id="')+9:data_string.find('data-l="')-2]
			else:
				broadcastid = ""
		except:
			broadcastid = ""
	except urllib.error.URLError as e:
		res = e.reason
		print(res)
		if res == 'Not Found':
			broadcastid = 'unknown'
			
	return broadcastid
	
def get_rtmp(id):
	req = urllib.request.Request(BROADCAST_URL + str(id))
	try:
		response = urllib.request.urlopen(req)
		r = response.read()
		soup = BeautifulSoup(r, 'html.parser')
		video_album = str(soup.find_all("a", class_="js-video-album-link"))
		username = video_album[video_album.find('>')+1:video_album.find('Live')]
		vp_video = str(soup.find_all("div", class_="vp_video"))
		rtmpstr = vp_video[vp_video.find('rtmpUrl'):]
		get_rtmp = rtmpstr[12:rtmpstr.find('\\\\')]
	except urllib.error.URLError as e:
		print("URLError: ",e.reason)
		get_rtmp = ''
	return (username, get_rtmp)

def rec_ffmpeg(broadcast_id, input, output):
	command = [FFMPEG,'-i' , input,'-y','-acodec','mp3','-loglevel','0', output]
	p[broadcast_id]=subprocess.Popen(command)
	broadcastdict[broadcast_id]['recording'] = 1
	time.sleep(1)
	
def convert2mp4(broadcast_id, input):
	if convertmp4 == 1:
		output = input.replace('.mkv','.mp4')
		command = [FFMPEG,'-i' , input,'-y','-loglevel','0', output]
		p1[broadcast_id]=subprocess.Popen(command)

while True:
	#read users.csv into list every loop, so you can edit csv file during run.
	print ('*--------------------------------------------------------------*')
	with open('users.csv', 'r') as readfile:
		reader = csv.reader(readfile, delimiter=',')
		usernames2 = list(reader)
	usernames = usernames2[0]
	deleteuserbroadcast = []
	for user in usernames:
		print ((time.strftime("%H:%M:%S")),' Polling ok account   :', user)
		broadcast_id = get_live_broadcast(user)
		if broadcast_id:
			if broadcast_id == 'unknown':
				usernames.remove(user)
				print ('Delete user: ', user)
				with open('users.csv', 'w') as outfile:
					writer = csv.writer(outfile, delimiter=',',quoting=csv.QUOTE_ALL)
					writer.writerow(usernames)
			else:
				if broadcast_id not in broadcastdict :
					broadcastid = str(broadcast_id)
					(username, URL) = get_rtmp(broadcastid)
					if len(URL) > 0:
						broadcastdict[broadcast_id] = {}
						broadcastdict[broadcast_id]['user'] = user
						broadcastdict[broadcast_id]['username'] = username
						broadcastdict[broadcast_id]['state']= 'RUNNING'
						broadcastdict[broadcast_id]['time']= time.time()
						broadcastdict[broadcast_id]['timelong']= time.strftime("%Y_%m_%d_%H%M%S")
						broadcastdict[broadcast_id]['filename']= username + '_ok_' + str(broadcastdict[broadcast_id]['timelong']) + '.mkv'
						broadcastdict[broadcast_id]['filesize']= 0
						broadcastdict[broadcast_id]['lasttime']= time.time()
						broadcastdict[broadcast_id]['recording']= 0
						print ('Start recording for: ', username)
						path = os.getcwd()
						if not os.path.exists(path + '/' + user):
							os.makedirs(path + '/' + user)
						output = path + '\\' + user + '\\' + broadcastdict[broadcast_id]['filename']
						rec_ffmpeg(broadcast_id, URL, output )
						time.sleep(8)
						if os.path.exists(output):
							print ('Recording started for: ', username, '-', broadcast_id)
						else:
							p[broadcast_id].terminate()
							print ('No recording file created for: ', user, 'file: ', broadcastdict[broadcast_id]['filename'])
							deleteuserbroadcast.append(broadcast_id)
	
	for broadcast_id in broadcastdict:
		#check if recording is running
		if p[broadcast_id].poll() == 0:
			broadcastdict[broadcast_id]['state'] = 'ENDED'
			deleteuserbroadcast.append(broadcast_id)
		else:
			print ('Running ',round(time.time()- broadcastdict[broadcast_id]['time']), 'seconds: ', broadcastdict[broadcast_id]['user'] , ' ' ,  broadcastdict[broadcast_id]['filename'])
			#compare file size every 60 seconds
			if os.path.exists(output) and broadcastdict[broadcast_id]['state'] == 'RUNNING':
				if broadcastdict[broadcast_id]['filesize'] < file_size(output) and (time.time() - broadcastdict[broadcast_id]['lasttime']) > 60:
					broadcastdict[broadcast_id]['filesize'] = file_size(output)
					broadcastdict[broadcast_id]['lasttime']= time.time()
				elif file_size(output) == broadcastdict[broadcast_id]['filesize'] and (time.time() - broadcastdict[broadcast_id]['lasttime']) > 60:
					p[broadcast_id].terminate()
					time.sleep(2)
					broadcastdict[broadcast_id]['state'] = 'ENDED'
					deleteuserbroadcast.append(broadcast_id)
	#end recording, delete entry in broadcastdict and convert mkv -> mp4
	for broadcast_id in deleteuserbroadcast:
		p[broadcast_id].terminate()
		print ('End recording for: ', broadcastdict[broadcast_id]['user'])
		if broadcast_id in broadcastdict:
			convert2mp4(broadcast_id, output)
			del broadcastdict[broadcast_id]
	time.sleep(1)