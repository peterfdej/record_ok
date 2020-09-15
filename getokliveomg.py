from bs4 import BeautifulSoup
import sys, time, os, getopt, csv
import os.path
import subprocess
import json
import urllib.request, urllib.error

OMG_OK_URL = 'http://www.liveomg.com/?filter=ok.ru'
OK_URL = 'https://ok.ru/profile/'
BROADCAST_URL = 'https://ok.ru/live/'
OK_URL_END = '/video?st._aid=NavMenu_User_Friend_Video'

SKIPFILE = 'skipuser.csv'
USERFILE = 'users.csv'
broadcastdict = {}
p = {}
p1 = {}
convertmp4 = 0
starturl = 1
maxurl = 90
userstart = 585000000000
userend   = 600000000000
rec_restr = 0 #1 for record restricted user
maxlenusers = 15

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
	
def get_oklive():
	req = urllib.request.Request(OMG_OK_URL)
	try:
		response = urllib.request.urlopen(req)
		r = response.read()
		response.close()
		soup = BeautifulSoup(r, 'html.parser')
		video_album = soup.find("ul", class_="webbies image-grid")
		result = {}
		for links in video_album.find_all('li'):
			link = str(links)
			ranking = str(link[link.find('<h')+4:link.find('</h')])
			broadcast_id = str(link[link.find('id-ok.ru-')+9:link.find('">')])
			result[ranking]={}
			result[ranking]['broadcast_id'] = broadcast_id
	except urllib.error.URLError as e:
		print("URLError: ",e.reason)
		result = {}
	return (result)

def get_restricted_broadcast(user):
	req = urllib.request.Request(OK_URL + user + OK_URL_END)
	broadcastid = ""
	try:
		response = urllib.request.urlopen(req)
		r = response.read()
		soup = BeautifulSoup(r, 'html.parser')
		if (soup.find(id='listBlockPanelFriendVideoLiveRBlock')):
			broadcastid = ""
		else:
			broadcastid = "Restricted"
	except urllib.error.URLError as e:
		res = e.reason
		print(res)
		if res == 'Not Found':
			broadcastid = 'unknown'
			
	return broadcastid
	
def get_rtmp(broadcasturl):
	req = urllib.request.Request(broadcasturl)
	try:
		response = urllib.request.urlopen(req)
		r = response.read()
		soup = BeautifulSoup(r, 'html.parser')
		video_album = str(soup.find_all("a", class_="js-video-album-link"))
		if ('/live/profile/') in video_album:
			userid = video_album[video_album.find('/live/profile/')+14:video_album.find('">')]
		else:
			userid = ''
		username = video_album[video_album.find('>')+1:video_album.find('Live')]
		vp_video = str(soup.find_all("div", class_="vp_video"))
		rtmpstr = vp_video[vp_video.find('rtmpUrl'):]
		get_rtmp = rtmpstr[12:rtmpstr.find('\\\\')]
	except urllib.error.URLError as e:
		print("URLError: ",e.reason)
		get_rtmp = ''
		userid = ''
		username = ''
	return (userid, username, get_rtmp)
	
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
	print ('*--------------------------------------------------------------*')
	result = get_oklive()
	#print(result)
	if result != {}:
		maxresult = 1
		newuser = 0
		deleteuserbroadcast = []
		with open(USERFILE, 'r') as readfile:
			reader = csv.reader(readfile, delimiter=',')
			usernames2 = list(reader)
			readfile.close
		usernames = usernames2[0]
		with open(SKIPFILE, 'r') as readfile:
			reader = csv.reader(readfile, delimiter=',')
			skipusers2 = list(reader)
		skipusers = skipusers2[0]
		for user in usernames:
			if user in skipusers:
				usernames.remove(user)
				print(user + " removed")
				newuser = 1
		for x in range(starturl, starturl + maxurl + 1):
			broadcast_id = result[str(x)]["broadcast_id"]
			broadcasturl = BROADCAST_URL + broadcast_id
			user_broadcast = get_rtmp(broadcasturl)
			user = user_broadcast[0]
			#check user in user.csv and skipuser.csv
			if user in usernames or user in skipusers or user == "" or int(user) < userstart or int(user) > userend:
				print("skip user ", user)
			else:
				if get_restricted_broadcast(user) == "Restricted" and broadcast_id not in broadcastdict:
					if rec_restr == 1:
						##record
						username = user_broadcast[1]
						broadcastdict[broadcast_id] = {}
						broadcastdict[broadcast_id]['user'] = user
						broadcastdict[broadcast_id]['username'] = username
						broadcastdict[broadcast_id]['URL'] = user_broadcast[2]
						broadcastdict[broadcast_id]['state']= 'RUNNING'
						broadcastdict[broadcast_id]['time']= time.time()
						broadcastdict[broadcast_id]['timelong']= time.strftime("%Y_%m_%d_%H%M%S")
						broadcastdict[broadcast_id]['filename']= username + '_ok_' + str(broadcastdict[broadcast_id]['timelong']) + '.mkv'
						broadcastdict[broadcast_id]['filesize']= 0
						broadcastdict[broadcast_id]['lasttime']= time.time()
						broadcastdict[broadcast_id]['recording']= 0
						print ('Start recording for: ', user)
						path = os.getcwd()
						if not os.path.exists(path + '/R' + user):
							os.makedirs(path + '/R' + user)
						output = path + '\\R' + user + '\\' + broadcastdict[broadcast_id]['filename']
						rec_ffmpeg(broadcast_id, broadcastdict[broadcast_id]['URL'], output )
						time.sleep(8)
						if os.path.exists(output):
							print ('Recording started for: ', username, '-', broadcast_id)
						else:
							p[broadcast_id].terminate()
							print ('No recording file created for: ', user, 'file: ', broadcastdict[broadcast_id]['filename'])
							deleteuserbroadcast.append(broadcast_id)
					else:
						print("Restricted user not recording")
				elif user in str(broadcastdict):
					print ("Restricted user recording")
				else:
					usernames.insert(0,user)
					print ('Add user: ', user)
					if len(usernames) > maxlenusers:
						usernames.pop()
					newuser = 1
			maxresult +=1
			if maxresult > maxurl:
				break
		if newuser == 1:
			with open(USERFILE, 'w') as outfile:
				writer = csv.writer(outfile, delimiter=',',quoting=csv.QUOTE_ALL)
				writer.writerow(usernames)
				outfile.close
			
		for broadcast_id in broadcastdict:
			#check if recording is running
			if p[broadcast_id].poll() == 0:
				broadcastdict[broadcast_id]['state'] = 'ENDED'
				deleteuserbroadcast.append(broadcast_id)
			else:
				print ('Running ',round(time.time()- broadcastdict[broadcast_id]['time']), 'seconds: ', broadcastdict[broadcast_id]['user'] , ' ' ,  broadcastdict[broadcast_id]['filename'])
				#compare file size every 60 seconds
				if os.path.exists(output) and broadcastdict[broadcast_id]['state'] == 'RUNNING':
					if broadcastdict[broadcast_id]['filesize'] < file_size(output) and (time.time() - broadcastdict[broadcast_id]['lasttime']) > 180:
						broadcastdict[broadcast_id]['filesize'] = file_size(output)
						broadcastdict[broadcast_id]['lasttime']= time.time()
					elif file_size(output) == broadcastdict[broadcast_id]['filesize'] and (time.time() - broadcastdict[broadcast_id]['lasttime']) > 180:
						p[broadcast_id].terminate()
						time.sleep(2)
						broadcastdict[broadcast_id]['state'] = 'ENDED'
						deleteuserbroadcast.append(broadcast_id)
		#end recording, delete entry in broadcastdict and convert mkv -> mp4
		for broadcast_id in deleteuserbroadcast:
			p[broadcast_id].terminate()
			#print ('End recording for: ', broadcastdict[broadcast_id]['user'])
			if broadcast_id in broadcastdict:
				convert2mp4(broadcast_id, output)
				del broadcastdict[broadcast_id]
			
	time.sleep(10)
