# -----------------------------------------------------------------------
# AUTHOR : LEONIDAS
# DATE : 26th APRIL 2016
# DESCRIPTION : CLASS TO SIMULATE DEVICE HEALTH MONITORING AND DEPLOYMENT
#------------------------------------------------------------------------

import os
import random
import math
import thread
from time import sleep
import sys
import shutil
import Merkle_Hash_Util

root_dir = '/home/leonidas/heart_beat1/'
fail_list = []
counter = 0
failure_trends = []
recovery_time = 1

#-----------------------------------------------------------------------------------
#  Prints Number of machines that were alive 
#  every second from the start of simulation
#-----------------------------------------------------------------------------------
def failure_trend():
    global failure_trends
    for trends in failure_trends:
        print failure_trends[trends]

#-----------------------------------------------------------------------------------
# Pings the value of failure trend list
# and updates the machine alive counter every second
#-----------------------------------------------------------------------------------
def check_alive():
    while (True):
        global root_dir, fail_list, counter, failure_trends
        count = 0
        sleep(1)
        for root, dirs, files in os.walk(root_dir):
            for name in dirs:
                if os.listdir(root_dir + name +"/"):
                    count += 1
        counter = count
        failure_trends.append(count)

#-----------------------------------------------------------------------------------
#  Adds a folder for each machine in the list and turns them alive
#-----------------------------------------------------------------------------------
def add_machine(machineNames):
    global root_dir, fail_list
    machineNames = machineNames.split(',')
    for machineName in machineNames:
        if not os.path.exists(root_dir + machineName):
            os.makedirs(root_dir + machineName)
            os.system("touch " + root_dir + machineName + "/alive.txt")


#-----------------------------------------------------------------------------------
# Removes the machines specified by the user
#-----------------------------------------------------------------------------------
def remove_machine(machineNames):
    machineNames = machineNames.split(',')
    for machineName in machineNames:
        global root_dir, fail_list
        if os.path.exists(root_dir + machineName):
            os.rmdir(root_dir + machineName)


#-----------------------------------------------------------------------------------
# Tickers every 5 seconds and calculates the number of machines that can fail
# and randomly turns them dead
#-----------------------------------------------------------------------------------
def remove_failed(fail_perc, reco_time):
    global root_dir, recovery_time

    while True:
        try:
            sleep(5)
            counts=0
            for root, dirs, files in os.walk(root_dir):
                for name in dirs:
                    counts += 1
            
            failed = math.ceil(int(fail_perc) * counts / 100)
            remove_dir = random.sample(os.listdir(root_dir),int(failed))
            
            for k in remove_dir:
               
                thread.start_new_thread(turn_alive,(k,recovery_time))
                # print "Removing   " + k
                os.remove(root_dir + k + "/alive.txt")
        except Exception as e:
            e = ''        
        
#-----------------------------------------------------------------------------------
# Starts as a new_thread invoked by remove_failed function
# which waits for specified recovery time and then turns the machine alive
#-----------------------------------------------------------------------------------
def turn_alive(machineName,reco_time):
        
    sleep(int(reco_time))  
    global root_dir
    os.system("touch " + root_dir + machineName + "/alive.txt")
    
#-----------------------------------------------------------------------------------
# Returns True or false based on the state of the machine
#-----------------------------------------------------------------------------------
def is_alive(machineName):
    global root_dir
    if os.path.exists(root_dir + machineName + "/alive.txt"):
        return True
    else:
        return not True


#-----------------------------------------------------------------------------------
# Returns a list of all the alive machines
#-----------------------------------------------------------------------------------
def all_alive():
    global root_dir
    areAlive=[]
    count = 0
    sleep(1)
    for root, dirs, files in os.walk(root_dir):
        for name in dirs:
            if os.listdir(root_dir + name +"/"):
                areAlive.append(name)
    
    return areAlive    

#-----------------------------------------------------------------------------------
# Deploys Files on machines that are alive
#-----------------------------------------------------------------------------------
def deploy(filesLocation):
    areAlive = all_alive()
    tocopy=os.listdir(filesLocation)
    not_deployed = set(os.listdir(root_dir)) - set(areAlive)
    deployed = False
    
    for folders in areAlive:
        for files in tocopy:
            if(not same_files(filesLocation, root_dir+folders+"/", files)):
                print "\n Deploying File: " + files + " on " + folders + "\n"
                shutil.copy(filesLocation+files,root_dir + folders)
    print "\n Deployed artifacts on all available machines. Waiting for machines to turn alive... \n"

    for left in not_deployed:
        while(not is_alive(left)):
            for files in tocopy:
                if(not same_files(filesLocation, root_dir+left+"/", files)):
                    print "\n Deploying File: " + files + " on " + left + "\n"
                    shutil.copy(filesLocation+files,root_dir + left) 


#-----------------------------------------------------------------------------------
# Function that uses Merkle_Hash_Util to compare 2 files and return the result
#-----------------------------------------------------------------------------------
def same_files(filesLocationSrc,filesLocationDst,files):
    hash_list_a = Merkle_Hash_Util.generate_sha1_for_file(filesLocationSrc,files)
    interim_hash_list_a= Merkle_Hash_Util.generate_merkle_hash(hash_list_a)
    if not os.path.exists(filesLocationDst+files):
        return False
    hash_list_b = Merkle_Hash_Util.generate_sha1_for_file(filesLocationDst,files)
    interim_hash_list_b= Merkle_Hash_Util.generate_merkle_hash(hash_list_b)

    if Merkle_Hash_Util.compare_hashes(interim_hash_list_a,interim_hash_list_b):
        return True
    else:
        os.rmdir(filesLocationDst+files)
        return False


#-----------------------------------------------------------------------------------
# Spawns up "N" number of machines on the start of simulation and turns them alive
#-----------------------------------------------------------------------------------
def initial_setup(N):
    global root_dir
    i = 1

    for i in xrange(int(N)):
        i= i+1

        machine_name = root_dir + "m_" + str(i)
        if not os.path.exists(machine_name):
            os.makedirs(machine_name)
            os.system("touch " + machine_name + "/alive.txt")




if __name__ == "__main__":

    Number = sys.argv[1]
    failure_percent = sys.argv[2]
    recovery_time = sys.argv[3]
    initial_setup(Number)

#------------------------------------------------
# Starting Parallel threads to invoke simulation
#------------------------------------------------
    
    thread.start_new_thread(check_alive, ())
    thread.start_new_thread(remove_failed,(failure_percent,recovery_time))
    # thread.start_new_thread(turn_alive,(recovery_time,))
    sleep(1)
    choice = ''
    print "******************************************************"
    print "*   Available Commands                               *"
    print "*----------------------------------------------------*"
    print "*  > add_machines m1,m2,....                         *"
    print "*  > remove_machines m_1, m2, ...                    *"
    print "*  > is_machine_alive m_1                            *"
    print "*  > num_machines_alive                              *"
    print "*  > failure_trend                                   *"
    print "*  > deploy <Files_location>                         *"
    print "******************************************************"
    while (choice != "quit"):
        choice = raw_input('enter command >')
        print choice
        if "add_machines" in choice:
            print "Adding "+ choice.split(' ')[1]
            add_machine(choice.split(' ')[1])
        elif choice.find("is_machine_alive") != -1:
            
            print is_alive(choice.split(' ')[1])
                # print True
        elif choice.find("remove_machines") != -1:
            print "Removing "+ choice.split(' ')[1]
            remove_machines(choice.split(' ')[1])

        elif choice.find("num_machines_alive") != -1:
            print "Machines Alive :" + str(counter)
        elif choice.find("failure_trend") != -1:
            print str(failure_trends)
        elif choice.find("deploy") != -1:
            deploy(choice.split(' ')[1])
                    

