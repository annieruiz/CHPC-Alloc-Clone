from __future__ import print_function
from TestBase   import TestBase
from util       import run_cmd, capture,syshost
from diskquota import diskquota

# this test reports disk usage and quota on home file systems
# it is utilizing a tool that accesses database with this info that is specific to CHPC

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
      diskquota()
      Flag=True
      return Flag
