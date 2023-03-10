from util import run_cmd, capture, syshost
from datetime import *
import re, sys, os
import shutil

class Cluster:
    def __init__(self, clusters):
        self.clusters = clusters
        self.path = ""

    def addPath(self, path):
        self.path = path


# Create objects for each of the clusters by name. This will include their paths, clusters, etc.
redwood = Cluster("redwood")
crystalpeak = Cluster("crystalpeak")
ondemand = Cluster(["kingspeak", "notchpeak", "lonepeak", "ash", "redwood", "crystalpeak", "scrubpeak"])
scrubpeak = Cluster("scrubpeak")
# ???? probably rename 'other', just not sure what it is right now.
other = Cluster(["kingspeak", "notchpeak", "lonepeak", "ash"])

# add sinfo paths to redwood, and ondemand
redwood.addPath("/uufs/redwood.bridges/sys/installdir/slurm/std/bin")
ondemand.addPath("/uufs/notchpeak.peaks/sys/installdir/slurm/std/bin")

hosts = {
    "redwood": redwood,
    "ondemand": ondemand,
    # first query for pe-ondemand since ondemand in host will be true there too & no notchpeak sys branch in the PE
    "pe-ondemand": redwood,
    "crystalpeak": crystalpeak,
    "scrubpeak": scrubpeak,
    "other": other
    }

cl = {
    "kingspeak": "kp",
    "notchpeak": "np",
    "ember": "em",
    "lonepeak": "lp",
    "ash": "smithp-ash",
    "redwood": "rw",
    "crystalpeak": "cp",
    "scrubpeak": "sp"
}

def allocations():
    h = syshost()
    if h in hosts:
        host = hosts[h]
        # print(f"syshost = {h}")
    else:
        if "ondemand" in h:
            host = hosts["ondemand"]
            # print(f"host is ondemand:  syshost = {h}")
        else:
            host = hosts["other"]
            # print(f"host is 'other':   syshost = {h}")

    """
     'shutil.which' returns the path to an exec which would be run if 'sinfo' was called. 
     'sinfo' returns information about the resources on the available nodes that make up the HPC cluster.
     """
    if shutil.which('sinfo') is None:
        if host.path == "":
            print("This command needs to run on one of the CHPC clusters")
            sys.exit(1)
        else:
            # os.environ["PATH"] is the equivalent to getenv("PATH") in C.
            os.environ["PATH"] += os.pathsep + host.path

    # primitive argument input for userid - no error checking
    if len(sys.argv) == 2:
        userid = sys.argv[1]
    else:
        userid = capture("whoami").rstrip()

    # userid="u1119546"
    # userid="u0631741"
    # redwood tests
    # userid="u6000771" XX
    # userid="u0413537"
    # userid="u6002243"

    """
    MC Jan 20
    A potentially cleaner version may be to create a list of account-QOS associations with 'sacctmgr' and then
    compare QOS from each of the association to the 'scontrol -o show partition' output to get the corresponding
    partition. 'scontrol' can be run only once with result stored in an array so that it's not run repeatedly.
    'sacctmgr -p show qos' lists what QOSes can this one preempt (Preempt = column 4), so we can see if
    preempt-able QOS is in this output, which would mean that it's preempt-able
    """

    grepcmd1 = "sacctmgr -n -p show assoc where user={0}".format(userid)
    # print(grepcmd1)
    myaccts = capture(grepcmd1).split()
    # print(myaccts,len(myaccts))

    clusters = host.clusters

    for cluster in clusters:
        FCFlag = True
        cl_ = cl[cluster]
        match_cl = [s for s in myaccts if cluster in s]

        # print(match_cl, len(match_cl))

        if match_cl:
            # first filter out owner accounts, this will be true if there are owner nodes
            if len(match_cl) > 1:
                match_str = "^((?!-{0}).)*$".format(cl_)
                # print(match_str)
                r = re.compile(match_str)
                match_cl = list(filter(r.match, match_cl))
                # print(match_cl)
                # print("Error, more than 1 match: {0}".format(match_cl))

            # now filter out the freecycle accounts
            match_fc = [s for s in match_cl if "freecycle" in s]
            if match_fc:
                # print(match_fc)
                for match_fc0 in match_fc:
                    p_names = match_fc0.split('|')
                    # print(p_names)
                    print(f"\tYour group \033[1;31m{p_names[1]}\033[0m does not have a "
                          f"\033[1;36mgeneral\033[0m allocation on \033[1;34m{cluster}\033[0m")
                    print(f"\tYou can use \033[1;33mpreemptable\033[0m mode on \033[1;34m{cluster}\033[0m. "
                          f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{p_names[17]}\033[0m")
                    print(
                        "\tYou can use \033[1;33mpreemptable\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], cluster + "-shared-freecycle"))

            # now look at allocated group accounts - so need to exclude owner-guest and freecycle
            matchg1 = [s for s in match_cl if not "freecycle" in s]
            # print(matchg1)
            matchg2 = [s for s in matchg1 if not "guest" in s]
            matchg3 = [s for s in matchg2 if not "collab" in s]
            # also filter out gpu accounts
            matchg4 = [s for s in matchg3 if not "gpu" in s]
            # matchg = [s for s in matchg2 if not "shared-short" in s]
            matchg = [s for s in matchg4 if not "notchpeak-shared" in s]
            if len(matchg) > 0:
                # print(matchg)
                for matchg1 in matchg:
                    # print(matchg1)
                    myrecord1 = matchg1.split('|')
                    # print(myrecord1)
                    print(
                        "\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m. Account: \033[1;32m{0}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            myrecord1[1], cluster, myrecord1[18]))
                    if (myrecord1[1] != "dtn"):  # account dtn that matches here does not have shared partition
                        print(
                            "\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{1}\033[0m. Account: \033[1;32m{0}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                                myrecord1[1], cluster, cluster + "-shared"))

        # shared-short
        matchgrp = [s for s in myaccts if "shared-short" in s]
        match_cl = [s for s in matchgrp if cluster in s]
        if len(match_cl) > 0:
            matchstr = "^((?!{0}).)*$".format(cl)
            r = re.compile(matchstr)
            match_cl = list(filter(r.match, match_cl))
            p_names = match_cl[0].split('|')
            print(
                "\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{1}\033[0m".format(
                    cluster, p_names[1]))

        # owner accounts
        # filter out owner accounts via Python list wrangling
        # matchown1 = [s for s in myaccts if any(xs in s for xs in [cluster, cl])]
        matchown1 = [s for s in myaccts if cluster in s]
        matchown2 = [s for s in matchown1 if cl in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # old logic with extra sacctmgr call
        # grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,cl)  # need to grep out guest since for ash cl=smithp-ash
        # print(grepcmd1)
        # print("myprojects")
        # myprojects=capture(grepcmd1).split()
        # print(myprojects,len(myprojects))
        grepcmd2 = "scontrol -M {0} -o show partition | grep -v shared".format(cluster)
        # print(grepcmd2)
        # allparts1=capture(grepcmd2)
        # print(allparts1)
        allparts = capture(grepcmd2).splitlines()
        # print(allparts)
        # testparts = subprocess.run(grepcmd2, stdout=subprocess.PIPE).stdout.decode('utf-8')
        # print(testparts)

        if len(myprojects) > 0:
            for project in myprojects:
                p_names = project.split('|')
                # print(pnames)
                # MC 1/24/20 - using scontrol to grep for partition that corresponds to the QOS in pnames[18]
                # example user that had QOS vs partition mix up - u6022494
                qosname = p_names[18]
                # in case "Def QOS" = pnames[18] is not defined, try "QOS" = pnames[17]
                if len(p_names[18]) == 0:
                    qosname = p_names[17]
                # print(qosname)
                # grepcmd2="scontrol -M {1} -o show partition | grep {0} | grep -v shared".format(qosname,cluster)
                # print(grepcmd2)
                # myparts=capture(grepcmd2).split()
                # print("myparts")
                # print(myparts,len(myparts))
                matchown1 = [s for s in allparts if qosname in s]
                if len(matchown1) > 0:
                    myparts = matchown1[0].split()
                    # print(myparts,len(myparts))

                    # print(myparts[0])
                    mypart = myparts[0].split('=')
                    # print(mypart[1])
                    pgroup = mypart[1].split('-')
                    print(
                        "\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], mypart[1]))
                    print(
                        "\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], pgroup[0] + "-shared-" + pgroup[1]))
                else:
                    print(
                        "\t\033[1;31mError:\033[0m you are in QOS \033[1;34m{0}\033[0m, but partition \033[1;32m{0}\033[0m does not exist. Please contact CHPC to fix this.".format(
                            qosname))

        # collab accounts
        matchown1 = [s for s in myaccts if cluster in s]
        matchown2 = [s for s in matchown1 if "collab" in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,"collab")  # need to grep out guest since for ash cl=smithp-ash
        # print(grepcmd1)
        # myprojects=capture(grepcmd1).split()
        # print("myprojects")
        # print(myprojects,len(myprojects))
        if len(myprojects) > 0:
            for project in myprojects:
                p_names = project.split('|')
                pgroup = p_names[17].split('-')
                # print(pnames)
                print(
                    "\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                        cluster, p_names[1], pgroup[0] + "-" + cl))
                print(
                    "\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                        cluster, p_names[1], pgroup[0] + "-shared-" + cl))

        # owner guest
        # have to get match_cl again since we may have changed it above
        match_cl = [s for s in myaccts if cluster in s]
        matchstr = ".*\\bguest\\.*"
        # print(matchstr)
        # print(match_cl, len(match_cl))
        r = re.compile(matchstr)
        myprojects = list(filter(r.match, match_cl))
        # print(myprojects)
        if len(myprojects) > 0:
            for project in myprojects:
                if "gpu" in project:
                    gpustr = " GPU"
                else:
                    gpustr = ""
                p_names = project.split('|')
                part = p_names[17].split(',')
                #    #print(pnames)
                print(
                    "\tYou can use \033[1;33mpreemptable{3}\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                        cluster, p_names[1], part[0], gpustr))

        # GPU accounts
        matchown1 = [s for s in myaccts if cluster in s]
        matchown2 = [s for s in matchown1 if "gpu" in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grepcmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w gpu | grep -v guest".format(userid,cluster)
        # print(grepcmd1)
        # myprojects=capture(grepcmd1).split()
        # print("myprojects")
        # print(myprojects,len(myprojects))
        if len(myprojects) > 0:
            for project in myprojects:
                p_names = project.split('|')
                # print(pnames)
                print(
                    "\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                        cluster, p_names[1], p_names[17]))

