"""
allocations3 will contain more slow, incremental changes, to know why allocations2.py isn't working
"""
import logging
import os
import re
import shutil
import sys
import argparse
from util import capture, syshost

"""
Create objects for each of the clusters by name. This dictionary will hopefully speed up
the script, since it's currently a bit slow and can take up to a few seconds to finish.
"""
clusters_ = {
    "redwood": ["redwood"],
    "ondemand": ["kingspeak", "notchpeak", "lonepeak", "ash",
                 "redwood", "crystalpeak", "scrubpeak"],
    # first query for pe-ondemand since ondemand in host will be true there too
    # and no notchpeak sys branch in the PE
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


def print_freecycle_allocs(user_, cluster_, partition_):
    if args.terse:
        for p in partition_:
            print(f"{cluster_} {user_}:{p}")
        return

    print(f"\tYour group \033[1;31m{user_}\033[0m does not have a"
          f"\033[1;36m general\033[0m allocation on \033[1;34m{cluster_}\033[0m")

    for p in partition_:
        print(f"\tYou can use\033[1;33m preemptable\033[0m mode on \033[1;34m{cluster_}\033[0m. "
              f"Account: \033[1;32m{user_}\033[0m, Partition: \033[1;32m{p}\033[0m")


def print_layout(color, alloc_type, cluster, user, partition):
    if args.terse:
        for p in partition:
            print(f"{cluster} {user}:{p}")
        return

    for p in partition:
        print(f"\tYou have a\033[{color} {alloc_type}\033[0m allocation on \033[1;34m{cluster}\033[0m. "
              f"Account: \033[1;32m{user}\033[0m, Partition: \033[1;32m{p}\033[0m")


# basic configuration for the logging system for debugging.
# Change the level to 'logging.DEBUG' to see the debug lines printed.
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

# add argument parsing functionality using argparse, the recommended
# command-line parsing module in the Python standard library.
parser = argparse.ArgumentParser()

# adds the 'terse' argument. Stores True or False, depending on whether terse is added or not.
parser.add_argument('-t', '--terse', help='decrease output verbosity',
                    action='store_true')

# adds an argument for the uID if the user would like to manually search one
parser.add_argument('user_id',
                    nargs='?',
                    help='see allocations for specific user id',
                    default=capture('whoami').rstrip())

args = parser.parse_args()


def allocations():
    host = syshost()
    if host in clusters_:
        clusters = clusters_[host]
        logging.debug(f'syshost = {host}')
    elif 'ondemand' in host:
        clusters = clusters_['ondemand']
        host = 'ondemand'
        logging.debug(f'host is ondemand:  syshost = {host}')
    else:
        clusters = clusters_['other']
        logging.debug(f'host is "other":   syshost = {host}')

    # 'shutil.which' returns the path to an exec which would be run if 'sinfo' was called.
    # 'sinfo' returns info on the resources on the available nodes that make up the HPC cluster.
    if shutil.which('sinfo') is None:
        if path[host]:    # equivalent to getenv("PATH") in C.
            os.environ['PATH'] += os.pathsep + path[host]
        else:
            print('This command needs to run on one of the CHPC clusters')
            sys.exit(1)

    user_id = args.user_id
    logging.debug(f'user_id: {user_id}')

    """
    test users:
        u1119546    u0631741
    redwood tests:
        u6000771    u0413537    u6002243
    
    MC Jan 20
    A potentially cleaner version may be to create a list of account-QOS associations with 'sacctmgr' and then compare
    QOS from each of the association to the 'scontrol -o show partition' output to get the corresponding partition.
    'scontrol' can be run only once with result stored in an array so that it's not run repeatedly. 
    'sacctmgr -p show qos' lists what QOSes can this one preempt (Preempt = column 4), so we can see if preempt-able
    QOS is in this output, which would mean that it's preempt-able
    """
    grep_cmd = f"sacctmgr -n -p show assoc where user={user_id}"
    logging.debug(f"grep_cmd: {grep_cmd}")
    all_accts = capture(grep_cmd).split()
    logging.debug(f"all_accts: {all_accts}, length: {len(all_accts)}")

    # for each cluster in the dictionary array
    for cluster in clusters:
        cl = cl_[cluster]
        cluster_accts = [s for s in all_accts if cluster in s]
        owner_guest_projects = [s for s in cluster_accts if "guest" in s]
        personal_projects = [s for s in cluster_accts if "guest" not in s]
        my_cluster_projects = [s for s in personal_projects if cl in s]
        my_eval_projects = [s for s in personal_projects if "eval" in s]
        my_collab_projects = [s for s in personal_projects if "collab" in s]
        my_gpu_projects = [s for s in personal_projects if "gpu" in s]

        logging.debug(f"match_cl: {cluster_accts}, length: {len(cluster_accts)}")
        # ------ general/freecycle accounts -------------------
        if cluster_accts:
            # first filter out owner accounts, this will be true if there are owner nodes
            if len(cluster_accts) > 1:
                logging.debug("multiple owners. We will filter out owner accounts.")
                regex = re.compile(f'^((?!-{cl}).)*$')
                cluster_accts = list(filter(regex.match, cluster_accts))
                logging.debug(f"new match_cl: {cluster_accts}")
            # ------ freecycle accounts -------------------
            freecycle_accts = [s for s in cluster_accts if 'freecycle' in s]
            for acct in freecycle_accts:
                p_names = acct.split('|')
                partitions = [p_names[17], cluster + '-shared-freecycle']
                logging.debug(p_names)
                print_freecycle_allocs(p_names[1], cluster, partitions)

            # ------ allocated group accounts -------------------
            acct_types = ['freecycle', 'guest', 'collab', 'gpu', 'eval', 'shared-short', 'notchpeak-shared']
            match_alloc_group = [s for s in cluster_accts if not any(x in s for x in acct_types)]
            for acct in match_alloc_group:
                my_record = acct.split('|')
                partitions = [my_record[18]]
                if my_record[1] != 'dtn':  # acct dtn that matches here does not have shared partition
                    partitions.append(cluster + '-shared')
                print_layout('1;36m', 'general', cluster, my_record[1], partitions)

            # ------ shared-short accounts -------------------
            shared_short_accts = [s for s in cluster_accts if 'shared-short' in s]
            for accts in shared_short_accts:
                p_names = accts.split('|')
                print_layout('1;36m', 'general', cluster, p_names[1], [p_names[1]])

        # ------ owner accounts -------------------
        grep_cmd2 = f"scontrol -M {cluster} -o show partition | grep -v shared"
        allparts = capture(grep_cmd2).splitlines()

        for project in my_cluster_projects:
            p_names = project.split('|')

            """
             MC 1/24/20 - using scontrol to grep for partition that corresponds to the QOS in pnames[18]
             example user that had QOS vs partition mix up - u6022494
            """
            qos_name = p_names[18] if p_names[18] else p_names[17]
            match_owner = [s for s in allparts if qos_name in s]

            if match_owner:
                myparts = match_owner[0].partition(' ')[0]
                mypart = myparts.partition('=')
                partition_name = mypart[2]
                logging.debug(f"partition_name = {partition_name[1]}")
                pgroup = partition_name.partition('-')
                partition = [partition_name, pgroup[0] + "-shared-" + pgroup[2]]
                if args.terse:
                    for p in partition:
                        print(f"{cluster} {p_names[1]}:{p}")
                else:
                    p = pgroup[0] + '-shared-' + pgroup[2]
                    print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. "
                          f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{partition_name}\033[0m")
                    print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. "
                          f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{p}\033[0m")
            else:
                print(f"\t\033[1;31mError:\033[0m you are in QOS \033[1;34m{qos_name}\033[0m, "
                      f"but partition \033[1;32m{qos_name}\033[0m does not exist. Please contact CHPC to fix this.")

        # ------ eval accounts -------------------
        for project in my_eval_projects:
            p_names = project.split('|')
            qos_name = p_names[18]
            # in case "Def QOS" = pnames[18] is not defined, try "QOS" = pnames[17]
            if len(p_names[18]) == 0:
                qos_name = p_names[17]
            match_owner1 = [s for s in allparts if qos_name in s]
            if len(match_owner1) > 0:
                myparts = match_owner1[0].split()
                mypart = myparts[0].split('=')
                if args.terse:
                    print("{0} {1}:{2}".format(cluster, p_names[1], mypart[1]))
                else:
                    print(f"\tYou have an \033[1;36mevaluation\033[0m allocation on \033[1;34m{cluster}\033[0m. "
                          f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{mypart[1]}\033[0m")
            else:
                print(f"\t\033[1;31mError:\033[0m you are in QOS \033[1;34m{qos_name}\033[0m, "
                      f"but partition \033[1;32m{qos_name}\033[0m does not exist. Please contact CHPC to fix this.")

        # ------ collab accounts -------------------
        for project in my_collab_projects:
            p_names = project.split('|')
            pgroup = p_names[17].split('-')
            partitions = [pgroup[0] + "-" + cl_[cluster], pgroup[0] + "-shared-" + cl_[cluster]]

            for p in partitions:
                if args.terse:
                    print(f"{cluster} {p_names[1]}:{p}")
                else:
                    print(f"\tYou have an \033[1;36mowner\033[0m allocation on \033[1;34m{cluster}\033[0m. "
                          f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{p}\033[0m")

        # ------ owner-guest accounts -------------------
        for project in owner_guest_projects:
            gpustr = ""
            if "gpu" in project:
                gpustr += " GPU"

            p_names = project.split('|')
            part = p_names[17].split(',')
            if args.terse:
                print("{0} {1}:{2}".format(cluster, p_names[1], part[0]))
            else:
                print(f"\tYou can use \033[1;33mpreemptable{gpustr}\033[0m mode on \033[1;34m{cluster}\033[0m. "
                      f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{part[0]}\033[0m")

        # ------ GPU accounts -------------------
        for project in my_gpu_projects:
            p_names = project.split('|')
            if args.terse:
                print("{0} {1}:{2}".format(cluster, p_names[1], p_names[17]))
            else:
                print(f"\tYou have a \033[1;36mGPU\033[0m allocation on \033[1;34m{cluster}\033[0m. "
                      f"Account: \033[1;32m{p_names[1]}\033[0m, Partition: \033[1;32m{p_names[17]}\033[0m")
