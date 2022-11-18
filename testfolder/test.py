import os
import sys
import glob
import time
import requests

def time_convert_and_write_to_file(sec, filename):
  mins = sec // 60
  sec = sec % 60
  hours = mins // 60
  mins = mins % 60
  f = open(os.path.join(sys.path[0], filename), 'a+')
  f.write("{0}:{1}:{2}\n".format(int(hours),int(mins),sec))
  f.close()

def CreateRequestWithFile(filename, url):
    with open(filename, 'rb') as f:
        r = requests.post(url, files={filename: f})
        return r


#Get files in current dir
#arr = os.listdir(sys.path[0])

Dict = {}
for file in glob.glob("*.txt"):
    with open(file, "r", encoding="utf8") as f:
        Dict[file] = f.read()

for key, value in Dict.items():
    #100 files of each:
    for i in range(1,101):
        #Starting time
        start_time = time.time()

        #Post Request
        res = CreateRequestWithFile(key, "someurl")

        #If status code is unsuccesful just print it
        if(is.not(res.ok)):
            print("Failed posting the file:")
            print(key)
        else:
            #Write the time to a file:
            end_time = time.time()
            time_lapsed = end_time - start_time
            time_convert_and_write_to_file(time_lapsed, "times-" + str(key))

            #Sleep some time before next request
            time.sleep(2)