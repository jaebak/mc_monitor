# Install auth-get-sso-cookie.
- Reference: https://gitlab.cern.ch/authzsvc/tools/auth-get-sso-cookie
Install commands
```
git clone ssh://git@gitlab.cern.ch:7999/authzsvc/tools/auth-get-sso-cookie.git
cd auth-get-sso-cookie
pip3 install --user .
## Example in using auth-get-sso-cookie
#export PATH=~/.local/bin${PATH:+:${PATH}}
#kinit
#auth-get-sso-cookie --url https://cms-pdmv-prod.web.cern.ch/mcm/ -o prod-cookie.txt
```

# Setup
Make a folder DATASET_FOLDER filled with the below files that have datasets.

Campaigns:
- RunIISummer20UL16NanoAODAPVv9, RunIISummer20UL16NanoAODv9
- RunIISummer20UL17NanoAODv9, RunIISummer20UL18NanoAODv9
- Run3Summer22NanoAODv11, Run3Summer22EENanoAODv11
- Run3Summer22NanoAODv12, Run3Summer22EENanoAODv12

Meaning of first char in line:
- #: Line is ignored
- !: Tag for datasets

An example input can be seen in the HToZGamma folder.

# Commands to run
```
source set_env.sh
get_mc_info.py -f DATASET_FOLDER -o DATASET_FOLDER
make_webpage.py -f DATASET_FOLDER - DATASET_FOLDER
```
An example output can be seen in [example_website/Run3Summer22NanoAODv12.html](https://htmlpreview.github.io/?https://github.com/jaebak/mc_monitor/blob/master/example_website/Run3Summer22NanoAODv12.html)
