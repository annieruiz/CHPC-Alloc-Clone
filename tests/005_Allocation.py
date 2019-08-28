from __future__ import print_function
from TestBase   import TestBase
from util       import run_cmd, capture,syshost
from datetime import *
import re

# this tests reports available SLURM accounts and partitions
# it is CHPC specific but the group and slurmdb queries should be fairly general

class Allocation(TestBase):
  
  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass

  def name(self):
    return "Check general, group and GPU allocation access"

  def description(self):
    return "Check general, group and GPU allocation access:"

  def error(self):
    print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
    print("\tPlease contact CHPC to fix your allocations.\n")

  def execute(self):
    host = syshost()
    #print(host)
    if (host!="kingspeak")and(host!="ember")and(host!="lonepeak")and(host!="notchpeak")and(host!="redwood"):
      return True

    Flag=False
    userid=capture("whoami").rstrip()
    #userid="u1119546"
    #userid="u0631741"
    # redwood tests
    #userid="u6000771"
    #userid="u0413537"
    #userid="u6002243"

    mycmd="groups {0}".format(userid)
    myout=capture(mycmd).split(":")
    groups=myout[1].split()

    grepcmd1="sacctmgr -n -p show assoc where user={0}".format(userid) 
    #print(grepcmd1)
    myaccts=capture(grepcmd1).split()
    #print(myaccts,len(myaccts))
    if host=="redwood":
      clusters=["redwood"]
    else:
      clusters=["kingspeak","notchpeak","ember","lonepeak"]
    for cluster in clusters:
#     grepcmd="grep %s %s" %(userid,projuser_map)
      FCFlag=True
      if cluster=="kingspeak":
	cl="kp"
      elif cluster=="notchpeak":
	cl="np"
      elif cluster=="ember":
	cl="em"
      elif cluster=="lonepeak":
	cl="lp"
      elif cluster=="redwood":
	cl="rw"
      for group in groups:
	# general allocation
	matchgrp = [s for s in myaccts if group in s]
	matchcl = [s for s in matchgrp if cluster in s]
	#print(matchcl, len(matchcl))
        if len(matchcl) > 0:
	  if (len(matchcl) > 1):
            # this will be true if there are owner nodes
	    matchstr="^((?!{0}).)*$".format(cl)  
	    #print(matchstr)
	    r=re.compile(matchstr)
	    matchcl = list(filter(r.match, matchcl))
	    #print(matchcl)
            #print("Error, more than 1 match: {0}".format(matchcl))
	  matchfc = [s for s in matchcl if "freecycle" in s]
	  if len(matchfc) > 0:
            pnames=matchfc[0].split('|')
            print("\tYour group \033[1;31m{0}\033[0m does not have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m".format(group,cluster))
	    print("\tYou can use \033[1;33mpreemptable\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17]))

	  else:	  
            myrecord1 = matchcl[0].split('|')
            #print(myrecord1)
            if myrecord1[1] == group:
              print("\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m. Account: \033[1;32m{0}\033[0m, Partition: \033[1;32m{1}\033[0m".format(group,cluster))
              Flag=True

      # owner nodes 
      # have to get matchcl again since we may have changed it above
     # matchcl = [s for s in myaccts if cluster in s]
     #matchstr=".*\\b{0}\\.*".format(cl)  
     #print(matchstr)
     #print(matchcl, len(matchcl))
     #r=re.compile(matchstr)
     #myprojects = list(filter(r.match, matchcl))
     #print(myprojects)
     #if len(myprojects) > 0:
     #  for project in myprojects:
     #    pnames=project.split('|')
     #    #print(pnames)
     #    print("\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17]))

      # the regex above does not match "em" as a single word (perhaps it's due to the filter() function
      # works fine on https://www.regextester.com/
      # so run the sacctmgr command again with grep -w
      grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2}".format(userid,cluster,cl) 
      #print(grepcmd1)
      myprojects=capture(grepcmd1).split()
      #print(myprojects,len(myprojects))
      if len(myprojects) > 0:
        for project in myprojects:
          pnames=project.split('|')
          #print(pnames)
	  print("\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17]))

      # owner guest
      # have to get matchcl again since we may have changed it above
      matchcl = [s for s in myaccts if cluster in s]
      matchstr=".*\\bowner\\.*"
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
     #    #print(pnames)
	  print("\tYou can use \033[1;33mpreemptable{3}\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17],gpustr))

      # GPU accounts
      grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w gpu | grep -v guest".format(userid,cluster) 
      #print(grepcmd1)
      myprojects=capture(grepcmd1).split()
      if len(myprojects) > 0:
        for project in myprojects:
          pnames=project.split('|')
          #print(pnames)
          print("\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(cluster,pnames[1],pnames[17]))
      
    if Flag: 
      print("\tSee https://www.chpc.utah.edu/usage/cluster/current-project-general.php for allocation usage information")
      return True
    else:  
      self.error_message+="\tError: All your allocations are invalid"
    return Flag     
