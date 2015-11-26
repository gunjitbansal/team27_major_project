from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
import threading
import time
import sys
import cgi, os
import subprocess
import cgitb
import re

fileServerPort = 6555
hdfsServer = "hdfs://localhost:54310"

""" ON GCE: Add property in hdfs-site.xml"""

params = len(sys.argv)
if params > 1:
	fileServerPort = int(sys.argv[1])
	if params > 2:
		hdfsServer = sys.argv[2]

class FileServer(BaseHTTPRequestHandler):

	def do_GET(self):
		self.send_response(200)
		self.end_headers()
		print self.request.getProperty("Test")
		print "Tried GET"
		return
	
	def do_POST(self):
		form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={'REQUEST_METHOD':'POST', 'CONTENT_TYPE':self.headers['Content-Type']})

		uploadedFile = form["uploadFile"]
		#print uploadedFile
		descr = form.getvalue("desc")
		print descr
		category = form.getvalue("catalog")
		print category
		
		url = ""
		if uploadedFile.filename:
			message =  "File uploaded successfully at: "+time.ctime(time.time())
			fileName = os.path.basename(uploadedFile.filename)
			print "filename"
			print fileName		
			url = hdfsServer+"/team27/"+fileName
			print "url---------->", url			
			open("/home/hduser/tmp/tempUpload", "wb").write(uploadedFile.file.read())
		
			with open("/home/hduser/tmp/tempUpload", "rb") as uploadFile:
				
				with open("/home/hduser/tmp/" + fileName, "wb") as uploadFileSave:
					schema = uploadFile.readline()[:-1]
					print "schema-------->", schema
					schema=schema.strip()
					print "schema-------- 1 >", schema
					schema=re.sub('[^A-Za-z0-9,]+','_',schema)
					print "schema-------- 2 >", schema
					descr=descr.strip();
					print "descr--------  >", descr

					descr=re.sub('[^A-Za-z0-9]+','_',descr)
					temp = uploadFile.readline()[:-1]
					print "temp--------  >", temp
					
					tempVal = temp.split(",")
					datatype=""
					flag = False
					for t in tempVal:
						try:
							x = int(t)
						except:
							try:
								x = float(t)
							except:
								x = str(t)
						y = type(x).__name__
						if flag == True:
							datatype = datatype + "," + y
						else:
							datatype = y
						flag = True
					uploadFileSave.write(temp+"\n")
					uploadFileSave.write(uploadFile.read())

			subprocess.call(["hdfs", "dfs", "-put", "/home/hduser/tmp/"+fileName, "/team27"])
			subprocess.call(["rm", "/home/hduser/tmp/tempUpload1"])
			subprocess.call(["rm", "/home/hduser/tmp/"+fileName])
		else:
			message =  "No file uploaded!"
		
		self.send_response(301)
		self.send_header("Method", "GET")
		self.send_header("Location", "http://localhost:8000/cloud/default/c1?descr="+descr+"&url="+url+"&schema="+schema+"&datatype="+datatype+"&category="+category)
		self.end_headers()
		#self.wfile.write(message)
		#self.wfile.write('\n')
		return

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
	"""Handle requests in a separate thread."""
	def shutdown(self):
		self.socket.close()
		HTTPServer.shutdown(self)

if __name__ == '__main__':
	try:
		fileServer = ThreadedHTTPServer(("", fileServerPort), FileServer)
		print "Starting server at http://0.0.0.0:" + str(fileServerPort)
		fileServer.serve_forever()
	except KeyboardInterrupt:
		print "\nTerminating Server... CYA!"
