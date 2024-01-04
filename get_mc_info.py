#!/usr/bin/env python3
import auth_get_sso_cookie.cern_sso
import json
import multiprocessing
# Below removes warning messages
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import traceback

import sys
import re
import os
import argparse

def get_info(campaign, dataset, category, args):
  success = False
  nTrial = 5
  for iTrial in range(nTrial):
    try:
      # datasets_info = {campaign: {dataset: {category, chains: [{chain, nevents, status, fragment, gwmsmon, priority, steps, last_reqmgr, n_running, n_idle
      #                                          prepips: [{prepip, completed_events, setup, das, mcm, prodmon, steps}]}]}
      #                            }}
      datasets_info = {campaign: {dataset: {}}}
      # Reference: https://cms-pdmv-prod.web.cern.ch/mcm/restapi
      session, response = auth_get_sso_cookie.cern_sso.login_with_kerberos(login_page='https://cms-pdmv-prod.web.cern.ch/mcm/', verify_cert=True, auth_hostname='auth.cern.ch', silent=False)
      print(f'Getting info for {dataset} in {campaign} trial={iTrial}')
      datasets_info[campaign][dataset]['category'] = category
      # Find chains with campaign
      dataset_url = f'https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/from_dataset_name/{dataset}'
      if args.debug: print(f'Getting {dataset_url}')
      dataset_json = session.get(dataset_url,verify=False).json()
      if args.debug: print(f'Got {dataset_url}')
      # dataset_json = {'results':[{'total_events', 'prepid', 'member_of_chain', 'member_of_campaign', 'status', 'priority', 'reqmgr_name': {'name'}}]}
      # request = {'total_events', 'prepid', 'member_of_chain', 'member_of_campaign', 'status', 'priority', 'reqmgr_name': {'name'}}
      found_requests = []
      for request in dataset_json['results']:
        if request['member_of_campaign'] != campaign: continue
        found_requests.append(request)
      # Fill info for chains
      datasets_info[campaign][dataset]['chains'] = []
      for request in found_requests:
        chains = request['member_of_chain']
        if len(chains) != 1: print(f'[Error] More than one chain for {dataset} with {campaign}')
        chain = chains[0]
        # Filter possible chains
        #print(chain)
        if re.search('NanoAODJME|NanoAODAPVJME',chain): continue
        if re.search('RECOmuonHits|RECOAPVmuonHits',chain): continue
        if re.search('ForTRK',chain): continue
        if re.search('DRALCA',chain): continue
        if re.search('forMuoVal',chain): continue
        if re.search('JMENano',chain): continue
        if args.debug: print(chain)
        if 'pilot' in request['process_string'].lower(): continue
        #print('  ',chain)
        nevents = request['total_events']
        priority = request['priority']
        # Find reqmgrs
        last_reqmgr = ''
        n_running = 0
        n_idle = 0
        for reqmgr in request['reqmgr_name']:
          last_reqmgr = reqmgr['name']
        if last_reqmgr != '': 
          gwmsmon = f'https://cms-gwmsmon.cern.ch/prodview/{last_reqmgr}'
          gwmsmon_url =f'https://cms-gwmsmon.cern.ch/prodview/json/{last_reqmgr}'
          if args.debug: print(f'Getting {gwmsmon_url}')
          gwmsmon_response = session.get(gwmsmon_url,verify=False)
          if gwmsmon_response.text.strip() != '':
            gwmsmon_json = gwmsmon_response.json()
            if args.debug: print(f'Got {gwmsmon_url}')
            n_running = gwmsmon_json['Running']
            n_idle = gwmsmon_json['Idle']
        else: gwmsmon = ''
        #print(chain, nevents, priority)
        # Find chain prepids
        chain_url = f'https://cms-pdmv-prod.web.cern.ch/mcm/restapi/chained_requests/get/{chain}'
        # chain_json = {'results': {'status', 'chain': []}}
        if args.debug: print(f'Getting {chain_url}')
        chain_json = session.get(chain_url,verify=False).json()
        if args.debug: print(f'Got {chain_url}')
        #print(f'{campaign}, {dataset}, {chain_json}')
        prepips = chain_json['results']['chain']
        status = chain_json['results']['status']
        lhe_prepip = prepips[0]
        if ('LHE' not in lhe_prepip) and ('GEN' not in lhe_prepip) and ('GS' not in lhe_prepip): print(f'[Error] no LHE in {lhe_prepip} for {dataset} in {campaign} in {chain}')
        fragment = f'https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_fragment/{lhe_prepip}'
        #print('  ',prepips, status, lhe_prepip, fragment, last_reqmgr, gwmsmon, priority)
        chain_info = {'chain':chain, 'nevents':nevents, 'status':status, 'fragment':fragment, 'gwmsmon':gwmsmon, 'priority':priority, 'last_reqmgr': last_reqmgr, 'n_running':n_running, 'n_idle': n_idle}
        chain_info['prepips'] = []
        all_steps = []
        # Find info for prepip
        for prepip in prepips:
          prepip_url = f'https://cms-pdmv-prod.web.cern.ch/mcm/restapi/requests/get/{prepip}'
          if args.debug: print(f'Getting {prepip_url}')
          prepip_json = session.get(prepip_url,verify=False).json()
          if args.debug: print(f'Got {prepip_url}')
          # prepip_json = {'results': {'sequences': [{'step', 'eventcontent'}], 'status', 'completed_events', 'output_dataset'}
          #print(prepip_json)
          prepip_eventcontents = []
          prepip_steps = []
          for sequence in prepip_json['results']['sequences']:
            #print(sequence['step'])
            prepip_eventcontents.append('_'.join(sequence['eventcontent']))
            if isinstance(sequence['step'], list): prepip_steps.append('_'.join(sequence['step']))
            elif isinstance(sequence['step'], str): prepip_steps.append(sequence['step'].replace(',','_'))
            else: print("[Error] Unknown step format: {sequence['step']}")
          prepip_eventcontent = '_'.join(prepip_eventcontents)
          prepip_step = '_'.join(prepip_steps)
          all_steps.append(prepip_step)
          prepip_status = prepip_json['results']['status']
          prepip_completed_events = prepip_json['results']['completed_events']
          prepip_setup = f'https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_test/{prepip}'
          prepip_mcm = f'https://cms-pdmv-prod.web.cern.ch/mcm/requests?prepid={prepip}'
          # If there are more than two datasets, take the later one
          #if len(prepip_json['results']['output_dataset']) >=2 : print(f"[Error] There are more than two output datasets: {prepip_json['results']['output_dataset']}")
          #prepip_output_dataset = prepip_json['results']['output_dataset'][0] if len(prepip_json['results']['output_dataset']) == 1 else ''
          prepip_output_dataset = '' if len(prepip_json['results']['output_dataset']) == 0 else prepip_json['results']['output_dataset'][-1]
          prepip_das = f'https://cmsweb.cern.ch/das/request?input={prepip_output_dataset}*' if prepip_output_dataset != '' else ''
          prepip_prodmon = f'https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_{prepip}' if ('LHE' in prepip) or ('NanoAOD' in prepip) else ''
          #print(prepip_json['results'])
          # Filter only for 
          #print('    ',prepip, prepip_step, prepip_eventcontent, prepip_status, prepip_completed_events, prepip_setup, prepip_output_dataset, prepip_das, prepip_mcm, prepip_prodmon)
          prepip_info = {'prepip':prepip, 'completed_events':prepip_completed_events, 'setup':prepip_setup, 'das': prepip_das, 'mcm': prepip_mcm, 'prodmon': prepip_prodmon, 'steps': prepip_step}
          chain_info['prepips'].append(prepip_info)
        all_step = '_'.join(all_steps)
        chain_info['steps'] = all_step
        #print('  ',all_step)
        #print(chain_info)
        datasets_info[campaign][dataset]['chains'].append(chain_info)
      success = True
    except Exception as e: 
       if args.debug: traceback.print_exc()
       print(f'[Info] Failed getting {dataset} {campaign} for trial {iTrial+1}/{nTrial}.')
    if success: break
  return datasets_info

if __name__== '__main__':
  parser = argparse.ArgumentParser(description='''\
Searches mcm.
Campaigns:
- RunIISummer20UL16NanoAODAPVv9, RunIISummer20UL16NanoAODv9
- RunIISummer20UL17NanoAODv9, RunIISummer20UL18NanoAODv9
- Run3Summer22NanoAODv11, Run3Summer22EENanoAODv11
- Run3Summer22NanoAODv12, Run3Summer22EENanoAODv12
''', formatter_class=argparse.RawTextHelpFormatter)

  parser.add_argument('-c','--campaign', help='Set a specific campaign', default=None)
  parser.add_argument('-f','--campaign_folder', help='Campaign folder', default='HToZGamma')
  parser.add_argument('-o','--output_folder', help='Output json folder', default='HToZGamma')
  parser.add_argument('-n','--number_cpus', help='Number of cpu to run on', default=8)
  parser.add_argument('-d', '--debug', action="store_true", help='Debug mode')

  args = parser.parse_args()

  if args.debug: args.number_cpus = 1

  #campaign_filename = 'RunIISummer20UL18NanoAODv9'
  #json_filename = 'mc_info.json'
  #campaign_filename = 'RunIISummer20UL16NanoAODAPVv9'
  #campaign_filename = 'Run3Summer22NanoAODv12'
  #campaign_folder = 'HToZGamma'
  campaign_folder = args.campaign_folder
  campaigns = ['RunIISummer20UL16NanoAODAPVv9', 'RunIISummer20UL16NanoAODv9', 'RunIISummer20UL17NanoAODv9', 
               'RunIISummer20UL18NanoAODv9',
               'Run3Summer22NanoAODv11', 'Run3Summer22EENanoAODv11',
               'Run3Summer22NanoAODv12', 'Run3Summer22EENanoAODv12',
               'Run3Summer23NanoAODv12', 'Run3Summer23BPixNanoAODv12',
               ]
  if args.campaign != None: campaigns = [args.campaign]
  #campaigns = ['RunIISummer20UL16NanoAODAPVv9']
  #campaigns = ['RunIISummer20UL16NanoAODv9']

  # datasets_info = {campaign: {dataset: {category, chains: [{chain, nevents, status, fragment, gwmsmon, priority, steps, last_reqmgr, n_running, idle
  #                                          prepips: [{prepip, completed_events, setup, das, mcm, prodmon, steps}]}]}
  #                            }}

  for campaign in campaigns:
    json_filename = os.path.join(campaign_folder,f'{campaign}.json')
    # Load existing json file
    if os.path.isfile(json_filename):
      with open(json_filename) as json_file:
        datasets_info = json.load(json_file)
    else:
      datasets_info = {}

    # Load campaign file
    campaign_filename = os.path.join(campaign_folder,campaign)
    if not os.path.isfile(campaign_filename): continue
    print(f'Opening {campaign_filename}')
    # campaign_datasets = {campaign: [[dataset, category]]}
    campaign_datasets = {}
    category = ''
    with open(campaign_filename) as input_file:
      for line in input_file:
        if line.strip() == '': continue
        if '#' in line.strip()[0]: continue
        if '!' in line.strip()[0]: 
          category = line.strip()[1:].strip()
          continue
        dataset_name = line.strip()
        if campaign not in campaign_datasets: campaign_datasets[campaign] = []
        campaign_datasets[campaign].append([dataset_name, category])

    #campaign_datasets = {'Run3Summer22NanoAODv12': [('DYGto2LG-1Jets_MLL-50_PTG-50to100_TuneCP5_13p6TeV_amcatnloFXFX-pythia8', 'background')]}
    #campaign_datasets = {'RunIISummer20UL18NanoAODv9': [('GluGluHToZG_ZToLL_M-125_TuneCP5_13TeV-powheg-pythia8','signal')]}
    #campaign_datasets = {'RunIISummer20UL17NanoAODv9': [('ZH_HToZG_ZToAll_M-125_TuneCP5_13TeV-powheg-pythia8', 'background')]}

    # Change categories according to campaign file
    for campaign in campaign_datasets:
      for dataset, category in campaign_datasets[campaign]:
        if campaign not in datasets_info: continue
        if dataset not in datasets_info[campaign]: continue
        t_category = datasets_info[campaign][dataset]['category']
        if t_category != category:
          datasets_info[campaign][dataset]['category'] = category


    # Make list of work
    work_arguments = []
    for campaign in campaign_datasets:
      for dataset, category in campaign_datasets[campaign]:
        # Check if there are any chains that are not done
        all_done = True
        if campaign not in datasets_info: all_done = False
        else:
          if dataset not in datasets_info[campaign]: all_done = False
          else:
            chains = datasets_info[campaign][dataset]['chains']
            if len(chains) == 0: all_done = False # Could not found chain
            for chain in chains:
              if chain['status'] != 'done':
                all_done = False
                break
        if all_done == False:
          print(f'Plan to search for {dataset} in {campaign}')
          work_arguments.append((campaign, dataset, category, args))

    # Get information with multiprocess
    ncpu = int(args.number_cpus)
    with multiprocessing.Pool(processes=ncpu) as pool:
      list_datasets_info = pool.starmap(get_info, work_arguments)
    #print(list_datasets_info)

    ## Get information with single process
    ## Make list of work
    #work_arguments = []
    #for campaign in campaign_datasets:
    #  for dataset, category in campaign_datasets[campaign]:
    #    work_arguments.append((campaign, dataset, category))
    #list_datasets_info = []
    #for campaign in campaign_datasets:
    #  for dataset, category in campaign_datasets[campaign]:
    #    list_datasets_info.append(get_info(campaign, dataset, category))

    # Combine datasets_info
    for temp_datasets_info in list_datasets_info:
      for campaign in temp_datasets_info:
        for dataset in temp_datasets_info[campaign]:
          dataset_info = temp_datasets_info[campaign][dataset]
          if campaign not in datasets_info: datasets_info[campaign] = {}
          if dataset in datasets_info[campaign]: print(f'[Warning] datasets_info already has {dataset} information')
          datasets_info[campaign][dataset] = dataset_info

    # Write to file
    #print(datasets_info)
    with open(json_filename, 'w', encoding='utf-8') as json_file:
      json.dump(datasets_info, json_file, ensure_ascii=False, indent=2)
      print(f'Made {json_filename}')
 
    ## Example of reading json file
    #with open(json_filename) as json_file:
    #  datasets_info_load = json.load(json_file)
    #print(datasets_info == datasets_info_load)
