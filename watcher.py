import time, requests, os.path, argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class Watcher:
	watch_folder = "/folder/to/watch"


	def __init__(self):
		self.observer = Observer()

	def run(self):
		event_handler = Handler()
		self.observer.schedule(event_handler, self.watch_folder, recursive=True)
		self.observer.start()
		try:
			while True:
				time.sleep(5)
		except:
			self.observer.stop()
			print("Error")

		self.observer.join()


class Handler(FileSystemEventHandler):
	parser = argparse.ArgumentParser()
	parser.add_argument("--s", help="Requesting from secured service", action = "store_true")
	args = parser.parse_args()
	file_list = list()
	d = {}
	if args.s:
		print("Requesting from secured service...")
		secure = 1
	else:
		print("Requesting from non-secured service, use --s for secured service")
		secure = 0

	@staticmethod
	def on_any_event(event):
		if event.is_directory:
			return None

		elif event.event_type == 'created':
			# If a raster dataset is added, upload it and add it to the Image Service:
			print("Received created event - %s." % event.src_path)
			# Delays for 10 seconds in case file is being uploaded/processed
			#time.sleep(10)
			Handler.d["file"] = event.src_path
			Handler.file_list.append(event.src_path)
			Handler.addRaster(event.src_path, Handler.secure)

		elif event.event_type == 'modified':
			# Do the following when a file is modified:
			print("Received modified event - %s." % event.src_path)
		
		elif event.event_type == 'deleted':
			# Do the following when a file is deleted:
			print("Received modified event - %s." % event.src_path)
			Handler.deleteRaster(event.src_path, Handler.secure)

			
	def getToken():
		#user = input("Username? ")
		#passwd = input("Password? ")
		user = "portaladmin"
		passwd = "portaladmin1"

		#url = 'https://www.arcgis.com/sharing/generateToken'
		url = 'https://machine.domain.com/portal/sharing/rest/generatetoken'
		login= {'username': user,
				   'password': passwd,
				   'referer': 'machine.domain.com',
				   'f': 'json'}
		# Note here that verify is set to False
		resp = requests.post(url, data=login, verify='/etc/pki/tls/certs/cert.pem') # Use verify=False if you don't have a certificate to use
		token = resp.json()['token']
		#aToken = token['token']
		print("Token obtained: " + token)
		return token

	def uploadItem(item, token):
		# Open file dropped in watched folder, upload multi-part... 
		file = open(item, 'rb')
		files = {'file': file}
		payload = {'f': 'json', 'token':token}
		url = 'https://machine.domain.com/server/rest/services/raster/isPy/ImageServer/uploads/upload'
		resp = requests.post(url, files=files, data=payload, verify='/etc/pki/tls/certs/cert.pem') # Use verify=False if you don't have a certificate to use


		if resp.status_code != 200:
		   # Output error code if something goes wrong...
		   raise ApiError('GET // {}'.format(resp.status_code))

		json_data = resp.json()
		itemID = json_data['item']['itemID']
		print(json_data)
		print(itemID)
		return(itemID)

	def addRaster(item, secure):
		if secure == 0:
			token = ""
		else:
			token = Handler.getToken()
		itemID = Handler.uploadItem(item, token)
		# Use Add Rasters to add uploaded items...
		# https://developers.arcgis.com/rest/services-reference/add-rasters.htm

		payload = {'f': 'json', 'itemIDs': itemID, 'computeStatistics': 'true', 'buildPyramids':'true', 'buildThumbnail':'true', 'attributes': '{"MinPS": 0, "MaxPS": 300}',
						'geodataTransformApplyMethod':'esriGeodataTransformApplyAppend','rasterType':'Raster Dataset',
						'token':token}

		url = 'https://machine.domain.com/server/rest/services/raster/isPy/ImageServer/add'
		resp = requests.post(url, data=payload, verify='/etc/pki/tls/certs/cert.pem') # Use verify=False if you don't have a certificate to use

		if resp.status_code != 200:
		   # Output error code if something goes wrong...
		   raise ApiError('GET // {}'.format(resp.status_code))

		json_data = resp.json()
		rasterID = json_data['addResults'][0]['rasterId']
		Handler.d["rasterID"] = rasterID
		Handler.file_list.append(rasterID)
		print(rasterID)
		print(json_data)
		print(Handler.file_list)

	def deleteRaster(item, secure):
		if secure == 0:
			token = ""
		else:
			token = Handler.getToken()

		del_index = Handler.file_list[Handler.file_list.index(item) + 1]

		payload = {'f': 'json', 'token':token, 'rasterIds':del_index}

		url = 'https://machine.domain.com/server/rest/services/raster/isPy/ImageServer/delete'
		resp = requests.post(url, data=payload, verify='/etc/pki/tls/certs/cert.pem') # Use verify=False if you don't have a certificate to use

		if resp.status_code != 200:
		   # Output error code if something goes wrong...
		   raise ApiError('GET // {}'.format(resp.status_code))	
		json_data = resp.json()
		print(json_data)


if __name__ == '__main__':
	w = Watcher()
	w.run()




