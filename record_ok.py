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
	try:
		response = urllib.request.urlopen(req)
		r = response.read()
		soup = BeautifulSoup(r, 'html.parser')
		page_container = soup.find(id='listBlockPanelFriendVideoLiveRBlock')
		if len(page_container) > 1:
			data_string = str(page_container)
			broadcastid = data_string[data_string.find('data-id="')+9:data_string.find('data-l="')-2]
		else:
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
					broadcastdict[broadcast_id] = {}
					broadcastdict[broadcast_id]['user'] = user
					broadcastdict[broadcast_id]['username'] = username
					broadcastdict[broadcast_id]['state']= 'RUNNING'
					broadcastdict[broadcast_id]['time']= time.time()
					broadcastdict[broadcast_id]['timelong']= time.strftime("%Y_%m_%d_%H%M%S")
					broadcastdict[broadcast_id]['filename']= user + '_ok_' + str(broadcastdict[broadcast_id]['timelong']) + '.mkv'
					broadcastdict[broadcast_id]['filesize']= 0
					broadcastdict[broadcast_id]['lasttime']= 0
					broadcastdict[broadcast_id]['recording']= 0
					print ('Start recording for: ', username)
					rec_ffmpeg(broadcast_id, URL, broadcastdict[broadcast_id]['filename'] )
					time.sleep(8)
					if os.path.exists(broadcastdict[broadcast_id]['filename']):
						print ('Recording started for: ', username, '-', broadcast_id)
					else:
						p[broadcast_id].terminate()
					if not os.path.exists(broadcastdict[broadcast_id]['filename']):
						print ('No recording file created for: ', user, 'file: ', broadcastdict[broadcast_id]['filename'])
						deleteuserbroadcast.append(broadcast_id)
	
	for broadcast_id in broadcastdict:
		#check if recording is running
		if p[broadcast_id].poll() == 0:
			broadcastdict[broadcast_id]['state'] = 'ENDED'
			deleteuserbroadcast.append(broadcast_id)
		else:
			print ('Running ',round(time.time()- broadcastdict[broadcast_id]['time']), 'seconds: ', broadcastdict[broadcast_id]['filename'])
			#compare file size every 60 seconds
			if os.path.exists(broadcastdict[broadcast_id]['filename']) and broadcastdict[broadcast_id]['state'] == 'RUNNING':
				if broadcastdict[broadcast_id]['filesize'] < file_size(broadcastdict[broadcast_id]['filename']) and (time.time() - broadcastdict[broadcast_id]['lasttime']) > 60:
					broadcastdict[broadcast_id]['filesize'] = file_size(broadcastdict[broadcast_id]['filename'])
					broadcastdict[broadcast_id]['lasttime']= time.time()
				elif file_size(broadcastdict[broadcast_id]['filename']) == broadcastdict[broadcast_id]['filesize'] and (time.time() - broadcastdict[broadcast_id]['lasttime']) > 60:
					p[broadcast_id].terminate()
					time.sleep(2)
					broadcastdict[broadcast_id]['state'] = 'ENDED'
					deleteuserbroadcast.append(broadcast_id)
	#end recording, delete entry in broadcastdict and convert mkv -> mp4
	for broadcast_id in deleteuserbroadcast:
		p[broadcast_id].terminate()
		print ('End recording for: ', broadcastdict[broadcast_id]['user'])
		if broadcast_id in broadcastdict:
			convert2mp4(broadcast_id, broadcastdict[broadcast_id]['filename'])
			del broadcastdict[broadcast_id]
	time.sleep(1)