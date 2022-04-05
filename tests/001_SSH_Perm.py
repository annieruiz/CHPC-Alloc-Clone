from __future__ import print_function
from TestBase import TestBase 
from util       import run_cmd, capture
import stat, os

class SSH_Perm(TestBase):
  
  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass

  def __write_group_other_test(self, path):
    #Return grpW/othw=true if the group and other write permission exist!
    st   = os.stat(path)
    grpW = bool(st.st_mode & stat.S_IWGRP)
    othW = bool(st.st_mode & stat.S_IWOTH)

    return [ { 'name' : 'group','value': grpW},
             { 'name' : 'other','value': othW},
           ]

  def __read_user_test(self, path):  
    #Return userR=true if the user read permission does NOT exist!
    st   = os.stat(path)
    userR = not bool(st.st_mode & stat.S_IRUSR)
    return [ { 'name' : 'user','value': userR}]

  def __authorized_keys_test(self, fn):
    st   = os.stat(fn)

    # test 600 permission on ~/.ssh/authorized_keys
    #perm = bool(st.st_mode & ( stat.S_IXUSR |
    #                           stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
    #                           stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH ))
    # CHPC has permissions 644, not 600
    perm = bool(st.st_mode & ( stat.S_IXUSR |
                               stat.S_IWGRP | stat.S_IXGRP |
                               stat.S_IWOTH | stat.S_IXOTH ))
    # if the file is not user readable
    perm = perm | (not bool(st.st_mode & stat.S_IRUSR))
    
    # test ownership
    own  = st.st_uid != os.getuid()
    #print(perm,own,st.st_uid,os.getuid())
    return perm or own

  def name(self):
    return "Check SSH permissions:"

  def description(self):
    return "Check SSH permissions:"

  def error(self):
    print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
    print(  "\tMake sure you have a .ssh directory under your $HOME directory.\n",
	    "\tYou can use the following commands to set the proper permissions:\n",
	    "\t$ chmod 700 $HOME   #(750 and 755 are also acceptable)\n",
	    "\t$ chmod 700 $HOME/.ssh\n",  
            "\t$ chmod 600 $HOME/.ssh/authorized_keys\n",
	    "\t$ chmod 600 $HOME/.ssh/id_rsa\n",
	    "\t$ chmod 644 $HOME/.ssh/id_rsa.pub\n",
#           "\t$ chown `whoami` $HOME/.ssh/authorized_keys\n"
            )

  def execute(self):
    result = True
  
 # CHPC's HOME is not in /etc/passwd so can't use it 
    home = os.environ['HOME']
    userid=capture("whoami").rstrip()
 #  grepcmd="grep %s /etc/passwd | cut -d ':' -f6" %userid
 #  grepcmd="/bin/awk -F: -v user=%s '$1 == user {print $6}' </etc/passwd" %userid
 #  home=capture(grepcmd)
    if not home:
        self.error_message+="\tError: Your home directory is inaccessible!\n"
        return False
 #   home=home[:-1]

    sshD = os.path.join(home,".ssh")
    dirA = [ home, sshD ]
    if not os.path.isdir(sshD):
      self.error_message+="\tError: .ssh directory does not exist or is inaccessible!\n"
      return False
    
    keyfile=os.path.join(sshD,"authorized_keys")
    if not os.path.isfile(keyfile):
      self.error_message+="\tError: Authorized key file does not exist or is inaccessible!\n"
      return False

# The following part is a little wordy, because we want to make some customized error messages:    
    a = self.__write_group_other_test(home)
    for entry in a:
      if (entry['value']):
        result = False
        self.error_message+="\tError: "+entry['name']+ " permission on $HOME will cause RSA to fail!\n"	  
#remove entry['name'] in the Error message on July 22, 2015  
 
    a = self.__read_user_test(home)
    for entry in a:
      if (entry['value']):
        result = False
        self.error_message+="\tError: no user read permission on $HOME will cause RSA to fail!\n"

# The following part is a little wordy, because we want to make some customized error messages:
    a = self.__write_group_other_test(sshD)
    for entry in a:
      if (entry['value']):
        result = False
        self.error_message+="\tError: "+entry['name']+ " permission on $HOME/.ssh will cause RSA to fail!\n"
#remove entry['name'] in the Error message on July 22, 2015

    a = self.__read_user_test(sshD)
    for entry in a:
      if (entry['value']):
        result = False
        self.error_message+="\tError: no user read permission on $HOME/.ssh will cause RSA to fail!\n"
 
    # test owner and perm on ~/.ssh/authorized_keys
    r = self.__authorized_keys_test(keyfile)
    if (r):
      result = False
      self.error_message+="\tError: The permission and/or ownership on " + keyfile +" will cause RSA to fail!\n"
      
    return result
