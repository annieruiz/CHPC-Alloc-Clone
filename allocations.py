from util       import run_cmd, capture,syshost
from datetime import *
import re, sys

def allocations():
  host = syshost()
  #print(host)
  if (host!="kingspeak")and(host!="ember")and(host!="lonepeak")and(host!="notchpeak")and(host!="ash")and(host!="redwood"):
    print("This command needs to run on one of the CHPC clusters")
    sys.exit(1)        
  
  # primitive argument input for userid - no error checking
  if len(sys.argv)==2:
    userid=sys.argv[1]
  else:
    userid=capture("whoami").rstrip()
  
  #userid="u1119546"
  #userid="u0631741"
  # redwood tests
  #userid="u6000771"
  #userid="u0413537"
  #userid="u6002243"
  
  # MC Jan 20
  # potentially cleaner version may be to create a list of account-QOS associations with sacctmgr
  # and then compare QOS from each of the association to the "scontrol -o show partition" output to get the corresponding partition
  # the scontrol can be run only once with result stored in an array so that it's not run repeatedly
  # sacctmgr -p show qos lists what QOSes can this one preempt (Preempt = column 4), can see if preemptable QOS is in this output which would mean that it's preemptable
  
  grepcmd1="sacctmgr -n -p show assoc where user={0}".format(userid) 
  #print(grepcmd1)
  myaccts=capture(grepcmd1).split()
  #print(myaccts,len(myaccts))
  if host=="redwood":
    clusters=["redwood"]
  else:
    clusters=["kingspeak","notchpeak","lonepeak","ash"]
  for cluster in clusters:
    FCFlag=True
    if cluster=="kingspeak":
      cl="kp"
    elif cluster=="notchpeak":
      cl="np"
    elif cluster=="ember":
      cl="em"
    elif cluster=="lonepeak":
      cl="lp"
    elif cluster=="ash":
      cl="smithp-ash"
    elif cluster=="redwood":
      cl="rw"
    matchcl = [s for s in myaccts if cluster in s]
    #print(matchcl, len(matchcl))
    if len(matchcl) > 0:
      if (len(matchcl) > 1):
        # first filter out owner accounts
        # this will be true if there are owner nodes
        matchstr="^((?!-{0}).)*$".format(cl)  
        #print(matchstr)
        r=re.compile(matchstr)
        matchcl = list(filter(r.match, matchcl))
        #print(matchcl)
        #print("Error, more than 1 match: {0}".format(matchcl))
      # now filter out the freecycle accounts
      matchfc = [s for s in matchcl if "freecycle" in s]
      if len(matchfc) > 0:
        #print(matchfc)
        for matchfc0 in matchfc:
          pnames=matchfc0.split('|')
          #print(pnames)
          print("\tYour group \033[1;31m{0}\033[0m does not have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m".format(pnames[1],cluster))
          print("\tYou can use \033[1;33mpreemptable\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17]))
          print("\tYou can use \033[1;33mpreemptable\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],cluster+"-shared-freecycle"))
  
      # now look at allocated group accounts - so need to exclude owner-guest and freecycle
      matchg1 = [s for s in matchcl if not "freecycle" in s]
      #print(matchg1)
      matchg2 = [s for s in matchg1 if not "guest" in s]
      matchg3 = [s for s in matchg2 if not "collab" in s]
      # also filter out gpu accounts
      matchg4 = [s for s in matchg3 if not "gpu" in s]
      #matchg = [s for s in matchg2 if not "shared-short" in s]
      matchg = [s for s in matchg4 if not "notchpeak-shared" in s]
      if len(matchg)>0:
        #print(matchg)
        for matchg1 in matchg:
  	#print(matchg1)
          myrecord1 = matchg1.split('|')
          #print(myrecord1)
          print("\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m. Account: \033[1;32m{0}\033[0m, Partition: \033[1;32m{1}\033[0m".format(myrecord1[1],cluster))
          print("\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m. Account: \033[1;32m{0}\033[0m, Partition: \033[1;32m{2}\033[0m".format(myrecord1[1],cluster,cluster+"-shared"))
  
  # shared-short
    matchgrp = [s for s in myaccts if "shared-short" in s]
    matchcl = [s for s in matchgrp if cluster in s]
    if len(matchcl) > 0:
      matchstr="^((?!{0}).)*$".format(cl)  
      r=re.compile(matchstr)
      matchcl = list(filter(r.match, matchcl))
      pnames=matchcl[0].split('|')
      print("\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{1}\033[0m".format(cluster,pnames[1]))
  
    # owner accounts
    grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,cl)  # need to grep out guest since for ash cl=smithp-ash
    #print(grepcmd1)
    myprojects=capture(grepcmd1).split()
    #print(myprojects,len(myprojects))
    if len(myprojects) > 0:
      for project in myprojects:
        pnames=project.split('|')
        #print(pnames)
        # MC 1/24/20 - using scontrol to grep for partition that corresponds to the QOS in pnames[18]
        # example user that had QOS vs partition mix up - u6022494
        qosname = pnames[18]
        # in case "Def QOS" = pnames[18] is not defined, try "QOS" = pnames[17]
        if len(pnames[18]) == 0:
          qosname = pnames[17]
        grepcmd2="scontrol -M {1} -o show partition | grep {0} | grep -v shared".format(qosname,cluster)
        #print(grepcmd2)
        myparts=capture(grepcmd2).split()
	if len(myparts) > 0:
          #print(myparts,len(myparts))
          #print(myparts[0])
          mypart=myparts[0].split('=')
          #print(mypart[1])
          pgroup=mypart[1].split('-')
          print("\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],mypart[1]))
          print("\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pgroup[0]+"-shared-"+pgroup[1]))
	else:
          print("\t\033[1;31mError:\033[0m you are in QOS \033[1;34m{0}\033[0m, but partition \033[1;32m{0}\033[0m does not exist. Please contact CHPC to fix this.".format(qosname))
          
  
    # collab accounts
    grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,"collab")  # need to grep out guest since for ash cl=smithp-ash
    #print(grepcmd1)
    myprojects=capture(grepcmd1).split()
    #print(myprojects,len(myprojects))
    if len(myprojects) > 0:
      for project in myprojects:
        pnames=project.split('|')
        pgroup=pnames[17].split('-')
        #print(pnames)
        print("\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pgroup[0]+"-"+cl))
        print("\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pgroup[0]+"-shared-"+cl))
  
    # owner guest
    # have to get matchcl again since we may have changed it above
    matchcl = [s for s in myaccts if cluster in s]
    matchstr=".*\\bguest\\.*"
   #print(matchstr)
    #print(matchcl, len(matchcl))
    r=re.compile(matchstr)
    myprojects = list(filter(r.match, matchcl))
    #print(myprojects)
    if len(myprojects) > 0:
      for project in myprojects:
        if "gpu" in project:
          gpustr = " GPU"
        else:
          gpustr = ""
        pnames=project.split('|')
        part=pnames[17].split(',')
   #    #print(pnames)
        print("\tYou can use \033[1;33mpreemptable{3}\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],part[0],gpustr))
  
    # GPU accounts
    grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w gpu | grep -v guest".format(userid,cluster) 
    #print(grepcmd1)
    myprojects=capture(grepcmd1).split()
    if len(myprojects) > 0:
      for project in myprojects:
        pnames=project.split('|')
        #print(pnames)
        print("\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17]))
    
