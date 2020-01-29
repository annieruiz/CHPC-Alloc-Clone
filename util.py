import subprocess
import os
import re

def capture(cmd):
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE,  stderr=subprocess.STDOUT, shell=True)
  return p.communicate()[0]

def captureErr(cmd):
  FNULL = open(os.devnull, 'w')
  p = subprocess.Popen(cmd, stdout=FNULL,  stderr=subprocess.PIPE, shell=True)
  return p.communicate()[1]
  
import subprocess

def run_cmd(cmd):
  return subprocess.call(cmd, shell=True)

import platform

def syshost():
  uufscell = os.getenv('UUFSCELL')

  if not uufscell:
    hostlong = capture("hostname --long")
    hostA = hostlong.split('.')
    idx = 0 # MC changed from 1 to 0
    if (len(hostA) < 2):
      idx = 0
  else:
    hostA = uufscell.split('.')
    idx = 0;

# MC also strip digits from the hostname
  return re.sub(r'\d+', '',hostA[idx])
