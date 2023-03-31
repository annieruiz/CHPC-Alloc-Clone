"""
allocations3 will contain more slow, incremental changes, to know why allocations2.py isn't working
"""
import logging
import os
import re
import shutil
import sys
from util import capture, syshost

# Create objects for each of the clusters by name.
clusters_ = {
    "redwood": ["redwood"],
    "ondemand": ["kingspeak", "notchpeak", "lonepeak", "ash", "redwood", "crystalpeak", "scrubpeak"],
    # first query for pe-ondemand since ondemand in host will be true there too & no notchpeak sys branch in the PE
    "pe-ondemand": ["redwood"],
    "crystalpeak": ["crystalpeak"],
    "scrubpeak": ["scrubpeak"],
    "other": ["kingspeak", "notchpeak", "lonepeak", "ash"]
}

path = {
    "redwood": "/uufs/redwood.bridges/sys/installdir/slurm/std/bin",
    "ondemand": "/uufs/notchpeak.peaks/sys/installdir/slurm/std/bin"
}

cl_ = {
    "kingspeak": "kp",
    "notchpeak": "np",
    "ember": "em",
    "lonepeak": "lp",
    "ash": "smithp-ash",
    "redwood": "rw",
    "crystalpeak": "cp",
    "scrubpeak": "sp"
}

def print_string_preemptable_mode(cluster_, acct_, partition_):
    for p in partition_:
        print (f"\tYou can use\033[1;33m preemptable\033[0m mode on \033[1;34m{cluster_}\033[0m. "
               f"Account: \033[1;32m{acct_}\033[0m, Partition: \033[1;32m{p}\033[0m")


def print_string_gen_alloc(cluster_, acct_, partition_):
    for p in partition_:
        print(f"\tYou have a\033[1;36m general\033[0m allocation on \033[1;34m{cluster_}\033[0m. "
              f"Account: \033[1;32m{acct_}\033[0m, Partition: \033[1;32m{p}\033[0m")


def print_string_no_gen_alloc(pname_, cluster_):
    print(f"\tYour group \033[1;31m{pname_}\033[0m does not have a"
          f"\033[1;36m general\033[0m allocation on \033[1;34m{cluster_}\033[0m")


def print_string_terse(pname_, cluster_, partition_):
    for p in partition_:
        print(f"{cluster_} {pname_}:{p}")


# basic configuration for the logging system for debugging
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

def allocations():
    host = syshost()
    if host in clusters_:
        clusters = clusters_[host]
        logging.debug(f"syshost = {host}")
    else:
        if "ondemand" in host:
            clusters = clusters_["ondemand"]
            host = "ondemand"
            logging.debug(f"host is ondemand:  syshost = {host}")
        else:
            clusters = clusters_["other"]
            logging.debug(f"host is 'other':   syshost = {host}")

    """
     'shutil.which' returns the path to an exec which would be run if 'sinfo' was called. 
     'sinfo' returns information about the resources on the available nodes that make up the HPC cluster.
     """
    if shutil.which('sinfo') is None:
        if path[host]:     # os.environ["PATH"] is the equivalent to getenv("PATH") in C.
            os.environ["PATH"] += os.pathsep + path[host]
        else:
            print("This command needs to run on one of the CHPC clusters")
            sys.exit(1)

    terse = False
    # primitive argument input for userid - no error checking
    if len(sys.argv) == 2:
        if sys.argv[1] in ("-t", "--terse"):
            terse = True
            userid = capture("whoami").rstrip()
        else:
            userid = sys.argv[1]
    else:
        userid = capture("whoami").rstrip()
    logging.debug(f"userid: {userid}")

    # userid="u1119546"
    # userid="u0631741"
    # redwood tests
    # userid="u6000771" XX
    # userid="u0413537"
    # userid="u6002243"

    """
    MC Jan 20
    A potentially cleaner version may be to create a list of account-QOS associations with 'sacctmgr' and then compare
    QOS from each of the association to the 'scontrol -o show partition' output to get the corresponding partition.
    'scontrol' can be run only once with result stored in an array so that it's not run repeatedly. 
    'sacctmgr -p show qos' lists what QOSes can this one preempt (Preempt = column 4), so we can see if preempt-able
    QOS is in this output, which would mean that it's preempt-able
    """

    grep_cmd1 = f"sacctmgr -n -p show assoc where user={userid}"
    logging.debug(f"grep_cmd: {grep_cmd1}")
    my_accts = capture(grep_cmd1).split()
    logging.debug(f"my_accts: {my_accts}, length: {len(my_accts)}")

    for cluster in clusters:
        FCFlag = True
        cl = cl_[cluster]
        match_cl = [s for s in my_accts if cluster in s]
        logging.debug(f"match_cl: {match_cl}, length: {len(match_cl)}")

        # ------ general/freecycle accounts -------------------
        if match_cl:
            # first filter out owner accounts, this will be true if there are owner nodes
            if len(match_cl) > 1:
                logging.debug("multiple owners. We will filter out owner accounts.")
                match_str = "^((?!-{0}).)*$".format(cl)
                logging.debug(f"match string: {match_str}")
                r = re.compile(match_str)
                match_cl = list(filter(r.match, match_cl))
                logging.debug(match_cl)

            # ------ freecycle accounts -------------------
            match_fc = [s for s in match_cl if "freecycle" in s]
            if match_fc:
                logging.debug(f"matchfc: {match_fc}")

                for match_fc0 in match_fc:
                    p_names = match_fc0.split('|')
                    partition = [p_names[17], cluster + "-shared-freecycle"]
                    logging.debug(f"pnames: {p_names}")

                    if terse:
                        print_string_terse(p_names[1], cluster, partition)
                    else:
                        print_string_no_gen_alloc(p_names[1], cluster)
                        print_string_preemptable_mode(p_names[1], cluster, partition)

            # ------ freecycle accounts -------------------
            # now look at allocated group accounts - so need to exclude owner-guest and freecycle
            matchg1 = [s for s in match_cl if "freecycle" not in s]
            logging.debug(f"matchg1: {matchg1}")
            filter_list = ["guest", "collab", "gpu", "eval", "shared-short", "notchpeak-shared"]
            # filter out gpu accounts, guest accounts, collab accts, eval, shared-short, and notchpeak-shared accts
            matchg2 = [s for s in matchg1 if "guest" not in s]
            matchg3 = [s for s in matchg2 if "collab" not in s]
            # also filter out gpu accounts
            matchg4 = [s for s in matchg3 if "gpu" not in s]
            matchg5 = [s for s in matchg4 if "eval" not in s]
            matchg6 = [s for s in matchg5 if "shared-short" not in s]
            matchg = [s for s in matchg6 if "notchpeak-shared" not in s]

            if matchg:
                logging.debug(f"matchg: {matchg}")
                for matchg1 in matchg:
                    logging.debug(f"matchg1: {matchg1}")
                    myrecord1 = matchg1.split('|')

                    partition = [myrecord1[18]]
                    if myrecord1[1] != "dtn":  # acct dtn that matches here does not have shared partition
                        partition.append(cluster + "-shared")

                    if terse:
                        print_string_terse(myrecord1[1], cluster, partition)
                    else:
                        print_string_gen_alloc(cluster, myrecord1[1], partition)

        # ------ shared-short accounts -------------------
        matchgrp = [s for s in my_accts if "shared-short" in s]
        match_cl = [s for s in matchgrp if cluster in s]
        if len(match_cl) > 0:
            match_str = "^((?!{0}).)*$".format(cl)
            r = re.compile(match_str)
            match_cl = list(filter(r.match, match_cl))
            p_names = match_cl[0].split('|')
            if terse:
                print("{0} {1}:{1}".format(cluster, p_names[1]))
            else:
                print(
                    "\tYou have a \033[1;36mgeneral\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{1}\033[0m".format(
                        cluster, p_names[1]))

        # ------ owner accounts -------------------
        # filter out owner accounts via Python list wrangling
        # matchown1 = [s for s in my_accts if any(xs in s for xs in [cluster, cl])]
        matchown1 = [s for s in my_accts if cluster in s]
        matchown2 = [s for s in matchown1 if cl in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # old logic with extra sacctmgr call
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,cl)  # need to grep out guest since for ash cl=smithp-ash
        # print(grep_cmd1)
        # print("myprojects")
        # myprojects=capture(grep_cmd1).split()
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
                    if terse:
                        print("{0} {1}:{2}".format(cluster, p_names[1], mypart[1]))
                        print("{0} {1}:{2}".format(cluster, p_names[1], pgroup[0] + "-shared-" + pgroup[1]))
                    else:
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

        # ------ eval accounts -------------------
        matchown1 = [s for s in my_accts if cluster in s]
        matchown2 = [s for s in matchown1 if "eval" in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print(myprojects)
        if len(myprojects) > 0:
            for project in myprojects:
                p_names = project.split('|')
                qosname = p_names[18]
                # in case "Def QOS" = pnames[18] is not defined, try "QOS" = pnames[17]
                if len(p_names[18]) == 0:
                    qosname = p_names[17]
                matchown1 = [s for s in allparts if qosname in s]
                if len(matchown1) > 0:
                    myparts = matchown1[0].split()
                    mypart = myparts[0].split('=')
                    pgroup = mypart[1].split('-')
                    if terse:
                        print("{0} {1}:{2}".format(cluster, p_names[1], mypart[1]))
                    else:
                        print(
                            "\tYou have an \033[1;36mevaluation\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                                cluster, p_names[1], mypart[1]))
                else:
                    print(
                        "\t\033[1;31mError:\033[0m you are in QOS \033[1;34m{0}\033[0m, but partition \033[1;32m{0}\033[0m does not exist. Please contact CHPC to fix this.".format(
                            qosname))

        # ------ collab accounts -------------------
        matchown1 = [s for s in my_accts if cluster in s]
        matchown2 = [s for s in matchown1 if "collab" in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,"collab")  # need to grep out guest since for ash cl=smithp-ash
        # print(grep_cmd1)
        # myprojects=capture(grep_cmd1).split()
        # print("myprojects")
        # print(myprojects,len(myprojects))
        if len(myprojects) > 0:
            for project in myprojects:
                p_names = project.split('|')
                pgroup = p_names[17].split('-')
                # print(pnames)
                if terse:
                    print("{0} {1}:{2}".format(cluster, p_names[1], pgroup[0] + "-" + cl))
                    print("{0} {1}:{2}".format(cluster, p_names[1], pgroup[0] + "-shared-" + cl))
                else:
                    print(
                        "\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], pgroup[0] + "-" + cl))
                    print(
                        "\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], pgroup[0] + "-shared-" + cl))

        # ------ owner-guest accounts -------------------
        # have to get matchcl again since we may have changed it above
        match_cl = [s for s in my_accts if cluster in s]
        match_str = ".*\\bguest\\.*"
        # print(matchstr)
        # print(matchcl, len(matchcl))
        r = re.compile(match_str)
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
                if terse:
                    print("{0} {1}:{2}".format(cluster, p_names[1], part[0]))
                else:
                    print(
                        "\tYou can use \033[1;33mpreemptable{3}\033[0m mode on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], part[0], gpustr))

        # ------ GPU accounts -------------------
        matchown1 = [s for s in my_accts if cluster in s]
        matchown2 = [s for s in matchown1 if "gpu" in s]
        myprojects = [s for s in matchown2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w gpu | grep -v guest".format(userid,cluster)
        # print(grep_cmd1)
        # myprojects=capture(grep_cmd1).split()
        # print("myprojects")
        # print(myprojects,len(myprojects))
        if len(myprojects) > 0:
            for project in myprojects:
                p_names = project.split('|')
                # print(pnames)
                if terse:
                    print("{0} {1}:{2}".format(cluster, p_names[1], p_names[17]))
                else:
                    print(
                        "\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                            cluster, p_names[1], p_names[17]))
