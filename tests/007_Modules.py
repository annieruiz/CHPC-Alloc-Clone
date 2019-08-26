from __future__ import print_function
from TestBase   import TestBase
from util       import run_cmd, capture,captureErr
import os,sys

class Modules(TestBase):
  
  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass

  def name(self):
      return "Check module environment"

  def description(self):
    return "Check module environment:"

  def error(self):
     print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
      print("\tPlease check your module environment.\n")

  def execute(self):
      Flag=True
      
      modcmd="tests/bash_module_test.bash"
      output=capture(modcmd)
      if "not found" in output:
	self.error_message+="\tError: Module is not defined."
        return False
      
      modcmd="tests/csh_module_test.csh"
      output=capture(modcmd)     
      if not output:
        self.error_message+="\tError: Module is not defined."
        return False

      unknown = "**UNKNOWN**"
      value = os.environ.get('LMOD_CMD',unknown)
      if (value == unknown):
	  self.error_message+="\tError: Module is not defined."
          return False             

      lmodcmd=os.environ['LMOD_CMD']
      mlcmd=lmodcmd + " python list"
      output=captureErr(mlcmd).split()
      module_need=["chpc"]
#     print(output)
      for mod1 in module_need:
	if not any(mod1 in tmpstr for tmpstr in output):
	  self.error_message+="\tError: Necessary module \"%s\" is not loaded.\n" %mod1
	  Flag=False
      
      return Flag
