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
  f.write("{0}\n".format(sec))
  f.close()

def CreateRequestWithFile(filename_, url):
    with open(filename_, 'rb') as f:
        test = f.read()
        start_time = time.time()
        r = requests.post(url, files={'file': test}, data={'filename': filename_})
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(elapsed_time)
    return r


#Get files in current dir
#arr = os.listdir(sys.path[0])

Dict = {}
for file in glob.glob("*.txt"):
    with open(file, "r", encoding="utf8") as f:
        Dict[file] = f.read()

for key, value in Dict.items():
    #100 files of each:
    for i in range(1,10):
        #Starting time
        start_time = time.time()

        #Post Request
        res = CreateRequestWithFile(key, "http://localhost:9000/files")
        
        #time_lapsed = end_time - start_time

        #If status code is unsuccesful just print it
        if(not res.ok):
            print("Failed posting the file:")
            print(key)
        else:
            pass
            #Write the time to a file:
            #print(time_lapsed)
            #time_convert_and_write_to_file(time_lapsed, "times-" + str(key))

            #Sleep some time before next request
            #time.sleep(2) 