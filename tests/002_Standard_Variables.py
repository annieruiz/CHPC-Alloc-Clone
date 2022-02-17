from __future__ import print_function
from TestBase   import TestBase 
from util       import run_cmd, capture, syshost
import os

class Standard_Variables(TestBase): 
  
  #Keep the error message and print it when necessary
  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass

  def name(self):
    return "Check pre-defined variables and corresponding file system accessibility"

  def description(self):
    return "Check environment variables (e.g. HOME) and file system access:"

  def error(self):
    print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
    print("\tPlease make sure necessary Environment Variables have been defined.\n")
  def execute(self):
    # Different variable are necessary on different machines.
    standardVarT = {
      'kingspeak' : [ "HOME", "UUFSCELL"],
      'notchpeak' :  [ "HOME"],
      'lonepeak' :  [ "HOME"],
      'redwood' :  [ "HOME"],
      'ember' :  [ "HOME"],
      'ash' :  [ "HOME"],
      'frisco' :  [ "HOME"],
      }

    host = syshost()
    #print(host)
    if host in standardVarT.keys():
      varA = standardVarT.get(host, standardVarT[host])
    else:
      self.error_message+="Not a known host."
      result = False
      return
 
    if not varA:
	return True

    result = True
    unknown = "**UNKNOWN**"

    for var in varA:
      value = os.environ.get(var,unknown)
      if (value == unknown):
	temp_string="\tError: Your $"+var+" is not defined.\n"
	self.error_message+=temp_string
        result = False
      elif( os.path.exists(value) ):
        pass
      else:
        if var != "UUFSCELL":
          temp_string="\tError: Your $" + var + " space(" + value + ") is not accessible at this time!\n"
          self.error_message+=temp_string
          result=False 	

#   fake test, ignore in the practical runs
#   mypath="/home/01225/siliu/no_exist_dir/"
#   if (os.path.exists(mypath) ):
#	print("my path exists")
#   else:
#	print("my path does not exist")
	
    return result
