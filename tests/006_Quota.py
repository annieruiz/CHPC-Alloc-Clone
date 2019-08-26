from __future__ import print_function
from TestBase   import TestBase
from util       import run_cmd, capture,syshost

class Quota(TestBase):
  
  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass

  def name(self):
    return "Check quota for $HOME space:"

  def description(self):
    return "Check quota for $HOME space:"

  def error(self):
     print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
      print("\tPlease remove unnecessary files to clean your space.\n")

  def execute(self):
      userid=capture("whoami").rstrip()
      #userid="u6006455"
      host=syshost()
      grepcmd1="curl --data 'unid={0}' https://www.chpc.utah.edu/apps/systems/curl_post/CHPCUserQuota.php -k -s | tail -n +2".format(userid)      
      myfs=capture(grepcmd1).splitlines()
      #print(myfs)
      spaces=["$HOME"]

      Flag=True

      goodcolor = "32m"
      badcolor = "31m"
      for space in myfs:
	  #print(space)
       # if ( (host=="ls4" and space=="/home1") or (host=="maverick" and space=="/home") or (host=="ls5" and space=="/home1")):
          rawinfo=space.split("\t")
	  if rawinfo[0]==userid:
            usage = rawinfo[1][:-4]
            files = rawinfo[3]
            quota = rawinfo[2][:-1]
            if quota=="0":
              badusage=100
            else:
              badusage=int(quota)
            if float(usage) > badusage:
              ucolor=badcolor
              umsg = "\033[1;31mare over quota limit\033[0m"
              if quota=="0":
                Flag=True
              else:
                Flag=False
            else:
              ucolor=goodcolor
              umsg = "have quota limit"
              Flag=True
            if int(files) > 100000:
              fcolor=badcolor
            else:
              fcolor=goodcolor
          
            if (rawinfo[2]=="0G"):
              print("\tOn \033[1;36m{0}\033[0m you have no quota limit, use \033[1;{3}{1} GB\033[0m and have \033[1;{4}{2}\033[0m files".format(rawinfo[0],usage,files,ucolor,fcolor))
            else:
              print("\tOn \033[1;36m{0}\033[0m you {6} \033[1;{7}{3} GB\033[0m, use \033[1;{4}{1} GB\033[0m and have \033[1;{5}{2}\033[0m files".format(rawinfo[0],usage,files,quota,ucolor,fcolor,umsg,ucolor))
      
      return Flag     
