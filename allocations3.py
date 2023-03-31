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
            match_freecycle = [s for s in match_cl if "freecycle" in s]
            for match_ in match_freecycle:
                p_names = match_.split('|')
                partition = [p_names[17], cluster + "-shared-freecycle"]
                logging.debug(f"p_names: {p_names}")

                if terse:
                    print_string_terse(p_names[1], cluster, partition)
                else:
                    print_string_no_gen_alloc(p_names[1], cluster)
                    print_string_preemptable_mode(p_names[1], cluster, partition)

            # ------ freecycle accounts -------------------
            # now look at allocated group accounts - so need to exclude owner-guest and freecycle
            match_group1 = [s for s in match_cl if "freecycle" not in s]
            logging.debug(f"match_group1: {match_group1}")
            filter_list = ["guest", "collab", "gpu", "eval", "shared-short", "notchpeak-shared"]
            # filter out gpu accounts, guest accounts, collab accts, eval, shared-short, and notchpeak-shared accts
            match_group2 = [s for s in match_group1 if "guest" not in s]
            match_group3 = [s for s in match_group2 if "collab" not in s]
            # also filter out gpu accounts
            match_group4 = [s for s in match_group3 if "gpu" not in s]
            match_group5 = [s for s in match_group4 if "eval" not in s]
            match_group6 = [s for s in match_group5 if "shared-short" not in s]
            match_alloc_group = [s for s in match_group6 if "notchpeak-shared" not in s]

            for match_ in match_alloc_group:
                logging.debug(f"match: {match_}")
                myrecord1 = match_.split('|')

                partition = [myrecord1[18]]
                # acct dtn that matches here does not have shared partition
                if myrecord1[1] != "dtn":
                    partition.append(cluster + "-shared")

                if terse:
                    print_string_terse(myrecord1[1], cluster, partition)
                else:
                    print_string_gen_alloc(cluster, myrecord1[1], partition)

        # ------ shared-short accounts -------------------
        match_shared_short = [s for s in my_accts if "shared-short" in s]
        match_cl = [s for s in match_shared_short if cluster in s]
        if match_cl:
            match_str = "^((?!{0}).)*$".format(cl)
            r = re.compile(match_str)
            match_cl = list(filter(r.match, match_cl))
            p_names = match_cl[0].split('|')
            if terse:
                print_string_terse(p_names[1], cluster, p_names[1])
            else:
                print_string_gen_alloc(cluster, p_names[1], p_names[1])


        # ------ owner accounts -------------------
        # filter out owner accounts via Python list wrangling
        # matchown1 = [s for s in my_accts if any(xs in s for xs in [cluster, cl])]
        match_owner1 = [s for s in my_accts if cluster in s]
        match_owner2 = [s for s in match_owner1 if cl in s]
        my_projects = [s for s in match_owner2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # old logic with extra sacctmgr call
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,cl)  # need to grep out guest since for ash cl=smithp-ash
        # print(grep_cmd1)
        # print("myprojects")
        # myprojects=capture(grep_cmd1).split()
        # print(myprojects,len(myprojects))
        grep_cmd2 = "scontrol -M {0} -o show partition | grep -v shared".format(cluster)
        # print(grepcmd2)
        # allparts1=capture(grepcmd2)
        # print(allparts1)
        allparts = capture(grep_cmd2).splitlines()
        # print(allparts)
        # testparts = subprocess.run(grepcmd2, stdout=subprocess.PIPE).stdout.decode('utf-8')
        # print(testparts)

        for project in my_projects:
            p_names = project.split('|')
            # print(pnames)

            # MC 1/24/20 - using scontrol to grep for partition that corresponds to the QOS in pnames[18]
            # example user that had QOS vs partition mix up - u6022494
            if len(p_names[18]) == 0:
                qos_name = p_names[17]  # in case "Def QOS" = pnames[18] is not defined, try "QOS" = pnames[17]
            else:
                qos_name = p_names[18]

            # print(qosname)
            # grepcmd2="scontrol -M {1} -o show partition | grep {0} | grep -v shared".format(qosname,cluster)
            # print(grepcmd2)
            # myparts=capture(grepcmd2).split()
            # print("myparts")
            # print(myparts,len(myparts))
            match_owner = [s for s in allparts if qos_name in s]
            if match_owner:
                myparts = match_owner[0].split()
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
                        qos_name))

        # ------ eval accounts -------------------
        match_owner1 = [s for s in my_accts if cluster in s]
        match_owner2 = [s for s in match_owner1 if "eval" in s]
        my_projects = [s for s in match_owner2 if not "guest" in s]
        # print(myprojects)
        for project in my_projects:
            p_names = project.split('|')
            qos_name = p_names[18]
            # in case "Def QOS" = pnames[18] is not defined, try "QOS" = pnames[17]
            if len(p_names[18]) == 0:
                qos_name = p_names[17]
            match_owner1 = [s for s in allparts if qos_name in s]
            if len(match_owner1) > 0:
                myparts = match_owner1[0].split()
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
                        qos_name))

        # ------ collab accounts -------------------
        match_owner1 = [s for s in my_accts if cluster in s]
        match_owner2 = [s for s in match_owner1 if "collab" in s]
        my_projects = [s for s in match_owner2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,"collab")  # need to grep out guest since for ash cl=smithp-ash
        # print(grep_cmd1)
        # myprojects=capture(grep_cmd1).split()
        # print("myprojects")
        # print(myprojects,len(myprojects))
        for project in my_projects:
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
        my_projects = list(filter(r.match, match_cl))
        # print(myprojects)
        for project in my_projects:
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
        match_owner1 = [s for s in my_accts if cluster in s]
        match_owner2 = [s for s in match_owner1 if "gpu" in s]
        my_projects = [s for s in match_owner2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w gpu | grep -v guest".format(userid,cluster)
        # print(grep_cmd1)
        # myprojects=capture(grep_cmd1).split()
        # print("myprojects")
        # print(myprojects,len(myprojects))
        for project in my_projects:
            p_names = project.split('|')
            # print(pnames)
            if terse:
                print("{0} {1}:{2}".format(cluster, p_names[1], p_names[17]))
            else:
                print(
                    "\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{0}\033[0m. Account: \033[1;32m{1}\033[0m, Partition: \033[1;32m{2}\033[0m".format(
                        cluster, p_names[1], p_names[17]))

