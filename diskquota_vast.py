from util import capture
import sys
import requests
def diskquota_vast():
    # primitive argument input for userid - no error checking
    if len(sys.argv)==2:
      username=sys.argv[1]
    else:
      username=capture("whoami").rstrip()

    GiB = 1024 * 1024 * 1024
    res = requests.get(f"https://portal.chpc.utah.edu/systems/storage/quota/~{username}")
    if not res:
        print("There was an error communicating with the quota server.")
        return
    BOLD = '\033[1m'
    CLEAR = '\033[0m'
    for path, metrics in res.json().items():
        user_quota_note = f" (out of {BOLD}{metrics['user_quota_bytes']/GiB} GiB{CLEAR} personal limit)" if metrics["user_quota_bytes"] != None else ""
        shared_quota_note = f" Overall capacity is {BOLD}{metrics['shared_usage_bytes']/metrics['shared_quota_bytes']*100:.2f}%{CLEAR} full." if metrics["shared_quota_bytes"] != None else ""
        print(f"In {path} you have used {BOLD}{metrics['user_usage_bytes']/GiB:.2f} GiB{CLEAR}{user_quota_note}.{shared_quota_note}")
