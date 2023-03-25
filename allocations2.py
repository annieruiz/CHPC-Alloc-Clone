import os
import re
import shutil
import sys
import logging

from util import capture, syshost

# basic configuration for the logging system for debugging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

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

""""
FREQUENTLY USED STRINGS
put them in their own method for ease of readability in the rest of the code.
"""
def string_no_gen_alloc(pname_, cluster_):
    return (f"\tYour group \033[1;31m{pname_}\033[0m does not have a"
            f"\033[1;36m general\033[0m allocation on \033[1;34m{cluster_}\033[0m")

def string_preemptable_mode_on(cluster_, acct_, partition_):
    return (f"\tYou can use\033[1;33m preemptable\033[0m mode on \033[1;34m{cluster_}\033[0m. "
            f"Account: \033[1;32m{acct_}\033[0m, Partition: \033[1;32m{partition_}\033[0m")

def string_gen_alloc(cluster_, acct_, partition_):
    str_ = ""
    for p in partition_:
        str_ += (f"\tYou have a\033[1;36m general\033[0m allocation on \033[1;34m{cluster_}\033[0m. "
                f"Account: \033[1;32m{acct_}\033[0m, Partition: \033[1;32m{partition_}\033[0m") + "\n"
    return str_


def allocations():
    host = syshost()
    if host in clusters_:
        clusters = clusters_[host]
        logging.debug(f"syshost = {host}")
    else:
        if "ondemand" in host:
            clusters = clusters_["ondemand"]
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
        cl_ = cl[cluster]
        match_cl = [s for s in my_accts if cluster in s]
        logging.debug(f"match_cl: {match_cl}, length: {len(match_cl)}")

        # ------ general/freecycle accounts -------------------
        if match_cl:
            # first filter out owner accounts, this will be true if there are owner nodes
            if len(match_cl) > 1:
                logging.debug("multiple owners. We will filter out owner accounts.")
                match_str = "^((?!-{0}).)*$".format(cl_)
                logging.debug(f"match string: {match_str}")
                r = re.compile(match_str)
                match_cl = list(filter(r.match, match_cl))
                logging.debug(match_cl)

            # now filter out the free-cycle accounts
            match_fc = [s for s in match_cl if "freecycle" in s]
            if match_fc:
                logging.debug(f"match_fc: {match_fc}")

                for match_fc0 in match_fc:
                    p_names = match_fc0.split('|')
                    logging.debug(f"p_names: {p_names}")
                    if terse:
                        print(f"{cluster} {p_names[1]} : {p_names[17]}")
                        print(f"{cluster} {p_names[1]} : {cluster}-shared-freecycle")
                    else:
                        print(string_no_gen_alloc(p_names[1], cluster))
                        print(string_preemptable_mode_on(cluster, p_names[1], p_names[17]))
                        print(string_preemptable_mode_on(cluster, p_names[1], cluster + "-shared-freecycle"))

            # ------------------- freecycle accounts -------------------
            # now look at allocated group accounts - so need to exclude owner-guest and freecycle
            match_g1 = [s for s in match_cl if "freecycle" not in s]
            logging.debug(f"match_g1: {match_g1}")
            filter_list = ["guest", "collab", "gpu", "eval", "shared-short", "notchpeak-shared"]
            # filter out gpu accounts, guest accounts, collab accts, and notchpeak-shared accts
            match_g = [s for s in match_g1 if not any(x in filter_list for x in s)]
            logging.debug(f"match_g: {match_g}")

            for match_g1 in match_g:
                my_record = match_g1.split('|')
                logging.debug(f"match_g1: {match_g1}, my_record:{my_record}")

                partitions = [my_record[18]]
                if my_record[1] == "dtn":   # account dtn that matches here has shared partition
                    partitions.append(cluster + "-shared")

                print(string_gen_alloc(cluster, my_record[1], partitions))

        # shared-short
        match_grp = [s for s in my_accts if "shared-short" in s]
        match_cl = [s for s in match_grp if cluster in s]

        # matches all accounts that have 'shared-short' and cluster in my_accts
        match_cl = [s for s in my_accts if all(x in ["shared-short", cluster] for x in s)]
        if match_cl:
            match_str = "^((?!{0}).)*$".format(cl_)
            r = re.compile(match_str)
            match_cl = list(filter(r.match, match_cl))
            p_names = match_cl[0].split('|')
            print(string_gen_alloc(cluster, p_names[1], p_names[1]))

        """
        OWNER ACCOUNTS
        filters out the owner accounts via Python list wrangling.
        """
        # match_own1 = [s for s in my_accts if any(xs in s for xs in [cluster, cl])]
        match_own1 = [s for s in my_accts if cluster in s]
        match_own2 = [s for s in match_own1 if cl_ in s]
        my_projects = [s for s in match_own2 if"guest" not in s]
        # old logic with extra sacctmgr call
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,cl)  # need to grep out guest since for ash cl=smithp-ash
        # print(grep_cmd1)
        # print("my_projects")
        # my_projects=capture(grep_cmd1).split()
        # print(my_projects,len(my_projects))
        grepcmd2 = "scontrol -M {0} -o show partition | grep -v shared".format(cluster)
        # print(grepcmd2)
        # allparts1=capture(grepcmd2)
        # print(allparts1)
        allparts = capture(grepcmd2).splitlines()
        # print(allparts)
        # testparts = subprocess.run(grepcmd2, stdout=subprocess.PIPE).stdout.decode('utf-8')
        # print(testparts)

        if my_projects:
            for project in my_projects:
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
                match_own1 = [s for s in allparts if qosname in s]
                if len(match_own1) > 0:
                    myparts = match_own1[0].split()
                    # print(myparts,len(myparts))
                    # print(myparts[0])
                    mypart = myparts[0].split('=')
                    # print(mypart[1])
                    pgroup = mypart[1].split('-')
                    print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. Account: "
                          f"\033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{mypart[1]}\033[0m")
                    print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. Account: "
                          f"\033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{pgroup[0]}-shared-{pgroup[1]}\033[0m")
                else:
                    print(f"\t\033[1;31mError:\033[0m you are in QOS \033[1;34m{0}\033[0m, but partition "
                          f"\033[1;32m{qosname}\033[0m does not exist. Please contact CHPC to fix this.")

        # collab accounts
        match_own1 = [s for s in my_accts if cluster in s]
        match_own2 = [s for s in match_own1 if "collab" in s]
        my_projects = [s for s in match_own2 if "guest" not in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w {2} | grep -v guest".format(userid,cluster,"collab")  # need to grep out guest since for ash cl=smithp-ash
        # print(grep_cmd1)
        # my_projects=capture(grep_cmd1).split()
        # print("my_projects")
        # print(my_projects,len(my_projects))
        if my_projects:
            for project in my_projects:
                p_names = project.split('|')
                pgroup = p_names[17].split('-')
                # print(pnames)
                print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. Account: "
                      f"\033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{pgroup[0]}-{cl_}\033[0m")
                print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. Account: "
                      f"\033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{pgroup[0]}-shared-{cl}\033[0m")

        # owner guest
        # have to get match_cl again since we may have changed it above
        match_cl = [s for s in my_accts if cluster in s]
        match_str = ".*\\bguest\\.*"
        # print(match_str)
        # print(match_cl, len(match_cl))
        r = re.compile(match_str)
        my_projects = list(filter(r.match, match_cl))
        # print(my_projects)
        if my_projects:
            for project in my_projects:
                if "gpu" in project:
                    gpustr = " GPU"
                else:
                    gpustr = ""
                p_names = project.split('|')
                part = p_names[17].split(',')
                #    #print(pnames)
                print(f"\tYou can use \033[1;33mpreemptable{gpustr}\033[0m mode on \033[1;34m{cluster}\033[0m. "
                      f"Account: \033[1;32m{p_names}\033[0m, Partition: \033[1;32m{part[0]}\033[0m")

        # GPU accounts
        match_own1 = [s for s in my_accts if cluster in s]
        match_own2 = [s for s in match_own1 if "gpu" in s]
        my_projects = [s for s in match_own2 if not "guest" in s]
        # print("matchown3")
        # print(matchown3,len(matchown3))
        # grep_cmd1="sacctmgr -p show assoc where user={0} | grep {1} | grep -w gpu | grep -v guest".format(userid,cluster)
        # print(grep_cmd1)
        # my_projects=capture(grep_cmd1).split()
        # print("my_projects")
        # print(my_projects,len(my_projects))
        if len(my_projects) > 0:
            for project in my_projects:
                p_names = project.split('|')
                # print(pnames)
                print(f"\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{cluster}\033[0m. Account: "
                      f"\033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{p_names[17]}\033[0m")
