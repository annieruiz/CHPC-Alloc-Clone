from __future__ import print_function
from TestBase   import TestBase
from util       import run_cmd, capture,syshost

class Schedulers(TestBase):
  
  error_message=""

  def __init__(self):
    pass

  def setup(self):
    pass
  
  def name(self):
    return "Check scheduler commands"

  def description(self):
    return "Check scheduler commands:"

  def error(self):
     print("\033[1;31m%s\033[0m" %(self.error_message))

  def help(self):
      print("\tPlease check your $PATH again, scheduler commands are missing.\n")

  def execute(self):
      host=syshost()
      if (host=="kingspeak" or host=="notchpeak" or host=="ember" or host == "lonepeak" or host == "redwood"):
        commands=["sbatch","squeue","scancel","scontrol"]
      #elif host=="ls4":
      #  commands=["qsub","qstat","qdel"]
      else:
        return True

      Flag=True

      for command1 in commands:
        typecmd="type %s" %command1 
        output=capture(typecmd)
        if "not found" in output:
          Flag=False
	  self.error_message+="\tError: Scheduler command \"%s\" is not available at this time!\n" %command1
      return Flag     
