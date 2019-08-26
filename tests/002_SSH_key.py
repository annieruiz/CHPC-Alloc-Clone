from __future__ import print_function
from TestBase   import TestBase 
from util       import run_cmd, capture
import os.path

class SSH_key(TestBase): 

  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass

  def name(self):
    return "Check SSH keys"

  def description(self):
    return "Check SSH keys:"

  def error(self):
    print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
    print("\tPlease check your .ssh directory, and append the contents of ~/.ssh/id_rsa.pub to ~/.ssh/authorized_keys.\n")

  def execute(self):
    result  = True
    pub_key = capture('cat ~/.ssh/id_rsa.pub').replace('\n','')
    cmd     = "grep -F \"%s\" ~/.ssh/authorized_keys > /dev/null 2>&1" % pub_key
    status  = run_cmd(cmd)

    if (status != 0):
      self.error_message+="\tInfo: ~/.ssh/id_rsa.pub not found in ~/.ssh/authorized_keys.\n"
      return False 

    cmd2    ="awk '{if ($1!=\"ssh-dss\" && $1!=\"ssh-rsa\" || NF <= 1) print $0}' ~/.ssh/authorized_keys"
    cmd2_out = capture(cmd2)
    if cmd2_out:
	self.error_message+="\tError: ~/.ssh/authorized_keys includes invalid or broken key(s).\n"
	return False;
     
    return result
