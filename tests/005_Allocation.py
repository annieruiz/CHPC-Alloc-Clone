from __future__ import print_function
from TestBase   import TestBase
from util       import run_cmd, capture,syshost
from allocations import allocations
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
    Flag=True
    allocations()
    return Flag
