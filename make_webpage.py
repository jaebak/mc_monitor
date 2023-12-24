#!/usr/bin/env python3
import json
import datetime
import re
import argparse
import os

def split_steps(steps):
  # Prevent HLT version from split
  find_hlt = re.findall('(HLT.*?_v\d*)',steps)
  if len(find_hlt)==0: step_string = steps
  else:
    hlt_old_string = find_hlt[0]
    hlt_new_string = hlt_old_string.replace('_','-')
    step_string = steps.replace(hlt_old_string, hlt_new_string)
  # Split steps
  t_steps = step_string.split('_')
  # Replace HTL version
  for istep, step in enumerate(t_steps):
    if 'HLT' in step: t_steps[istep] = step.replace('-','_')
  return t_steps

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='''\
Makes webpage.
Campaigns:
- RunIISummer20UL16NanoAODAPVv9, RunIISummer20UL16NanoAODv9
- RunIISummer20UL17NanoAODv9, RunIISummer20UL18NanoAODv9
- Run3Summer22NanoAODv11, Run3Summer22EENanoAODv11
- Run3Summer22NanoAODv12, Run3Summer22EENanoAODv12
''', formatter_class=argparse.RawTextHelpFormatter)

  parser.add_argument('-c','--campaign', help='Set a specific campaign', default=None)
  parser.add_argument('-f','--campaign_folder', help='Campaign json folder', default='HToZGamma')
  parser.add_argument('-o','--output_folder', help='Output html folder', default='HToZGamma')
  parser.add_argument('-n','--number_cpus', help='Number of cpu to run on', default=8)

  args = parser.parse_args()

  campaign_folder = args.campaign_folder
  campaigns = ['RunIISummer20UL16NanoAODAPVv9', 'RunIISummer20UL16NanoAODv9', 'RunIISummer20UL17NanoAODv9', 
               'RunIISummer20UL18NanoAODv9',
               'Run3Summer22NanoAODv11', 'Run3Summer22EENanoAODv11',
               'Run3Summer22NanoAODv12', 'Run3Summer22EENanoAODv12'
               ]
  if args.campaign != None: campaigns = [args.campaign]

  for campaign in campaigns:
    json_filename = os.path.join(campaign_folder,f'{campaign}.json')
    web_filename = os.path.join(campaign_folder,f'{campaign}.html')
    web_file = open(web_filename,'w')
    print(f'Making {web_filename}')

  
    # datasets_info = {campaign: {dataset: {category, chains: [{chain, nevents, status, fragment, gwmsmon, priority, steps, last_reqmgr, n_running, n_idle
    #                                          prepips: [{prepip, completed_events, setup, das, mcm, prodmon, steps}]}]}
    #                            }}
    datasets_info = {}
    with open(json_filename) as json_file:
      datasets_info = json.load(json_file)
  
    # Find all campaigns
    #campaigns = list(datasets_info.keys())
    # Find all categories
    # categories = {category: ndataset}
    categories = {}
    for dataset in datasets_info[campaign]:
      category = datasets_info[campaign][dataset]['category']
      nchains = len(datasets_info[campaign][dataset]['chains'])
      if nchains == 0: nchains +=1 # For case of no found sample
      if category not in categories: 
        categories[category] = nchains
      else: 
        categories[category] += nchains
    #print(categories)
  
    # Find all steps
    steps = []
    for dataset in datasets_info[campaign]:
      for chain in datasets_info[campaign][dataset]['chains']:
        #print('input',chain['steps'])
        t_steps = split_steps(chain['steps'])
        if steps == []: steps = t_steps
        # Combine steps from different chains
        istep = 0
        for step in t_steps:
          if step in steps: istep = steps.index(step)
          else: 
            steps.insert(istep+1, step)
            #print('putting',step,'in',istep)
    #print(steps)
  
    # Find reduced steps
    edges = [0]*len(steps)
    for dataset in datasets_info[campaign]:
      for chain in datasets_info[campaign][dataset]['chains']:
        for prepip in chain['prepips']:
          step = prepip['steps']
          step_list = split_steps(step)
          #print(step_list)
          edges[steps.index(step_list[0])] = 1
          if len(step_list) != 1: 
            edges[steps.index(step_list[-1])] = 1
    reduced_steps = []
    for iedge, edge in enumerate(edges):
      if edge == 1: reduced_steps.append(steps[iedge])
    #print(steps)
    #print(reduced_steps)
  
  
    web_file.write('''\
<html>
<style>
table, th, td { border: 1px solid black; }
table  { border-collapse: collapse; }
td  { font-size: 100%; padding-bottom: 0.33em; padding-top: 0.33em; padding-left: 0.33em; padding-right: 0.33em; }
a { text-decoration: none; }
.withBG     { background-image: linear-gradient(#FB00FF, #FB00FF, #FB00FF); background-repeat: no-repeat;}
.withBGdone { background-image: linear-gradient(#87FF00, #87FF00, #87FF00); background-repeat: no-repeat;}
.withBGval { background-image: linear-gradient(#C18080, #C18080, #C18080); background-repeat: no-repeat;}
</style>
''')
    web_file.write(f'<h1>{campaign}</h1>\n')
    # Write campaigns
    web_line = '<p>\n'
    for t_campaign in campaigns:
      web_line += f'<a href="{t_campaign}.html">{t_campaign}</a>, '
    web_line += f'(update: {datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")})\n</p>'
    web_file.write(web_line)
    # Write categories
    web_line = '<p>\n'
    for category in categories:
      web_line += f'<a stype="text-decoration: none" href="#{category}"> {category} </a>,&nbsp&nbsp\n'
    web_line = web_line[:-12]+'</p>\n'
    web_file.write(web_line)
  
    # Make table header
    web_file.write('''\
<table>
<tr>
<td>Type</td>
<td>Dataset</td>
<td>nEvt(chain)</td>
<td>Priority</td>
<td><span title="Status">S</span>(<span title="Run">R</span>/<span title="Idle">I</span>)</td>
  ''')
    for step in reduced_steps:
      ## Add space
      #nspace=0
      ##print(step, len(step))
      #if len(step) < 3:
      #  nspace = 3-len(step)
      #add_space = '&nbsp;'*nspace
      #print(nspace, add_space)
      #web_file.write(f'<td>{step}{add_space}</td>\n')
      step_string = step if step != 'PAT' else 'MINI'
      web_file.write(f'<td><span title="{step_string}">{step_string[:1]}</span></td>\n')
    web_file.write('</tr>\n')
  
    for category in categories:
      web_file.write('<tr>\n')
      web_file.write(f'<td rowspan="{categories[category]}" width="100px"  id="{category}"  > <a  style="color:black;text-decoration: none" href="#{category}"> {category} </a></td>\n')
  
      for dataset in datasets_info[campaign]:
        if category != datasets_info[campaign][dataset]['category']: continue
        chains = datasets_info[campaign][dataset]['chains']
        if len(chains) == 0:
          web_file.write(f'<td rowspan=1> {dataset} </td>\n')
          web_file.write(f'<td colspan={len(reduced_steps)+3}> No sample </td>\n')
          web_file.write('</tr>\n')
        else:
          web_file.write(f'<td rowspan={len(chains)}> {dataset} </td>\n')
          for chain in chains:
            nevent_string = f'{chain["nevents"]/1000000:.1f}M'
            web_file.write(f'<td> <a href="https://cms-pdmv-prod.web.cern.ch/mcm/requests?member_of_chain={chain["chain"]}&page=0&shown=4398046511103"> <span title="{chain["nevents"]}">{nevent_string}</span> </a> </td>\n')
            if chain["status"] == 'done':
              web_file.write(f'<td colspan=2> {chain["status"]} </td>\n')
            else:
              priority_string = f'{chain["priority"]/1000:.0f}K'
              #web_file.write(f'<td> <span title="{chain["priority"]}">{priority_string}</span> </td>\n')
              web_file.write(f'<td> <a href=https://cms-pdmv-prod.web.cern.ch/stats/?workflow_name={chain["last_reqmgr"]}> <span title="{chain["priority"]}">{priority_string}</span> </a> </td>\n')
              web_file.write(f'<td> <a href=https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/{chain["last_reqmgr"]}><span title="{chain["status"]}">{chain["status"][0].upper()}</span></a> <a href="{chain["gwmsmon"]}">({chain["n_running"]}/{chain["n_idle"]})</a> </td>\n')
            # Fill steps
            filled_step_index = 0
            for prepip in chain['prepips']:
              steps = split_steps(prepip['steps'])
              # Count columns to fill
              step_string = steps[0]
              nsteps = 1
              if len(steps) != 1: 
                nsteps = reduced_steps.index(steps[-1]) - reduced_steps.index(steps[0]) + 1
                step_string = f'{steps[0]} - {steps[-1]}'
              # Count blank columns to fill
              n_empty = 0
              first_fill = True
              if reduced_steps.index(steps[0]) != filled_step_index:
                n_empty = reduced_steps.index(steps[0]) - filled_step_index
              #print(f'n_empty: {n_empty}, filled_step_index: {filled_step_index}, steps[0]: {steps[0]}, steps[-1]: {steps[-1]}')
              filled_step_index = reduced_steps.index(steps[0])+nsteps
              # Fill blank columns
              if n_empty != 0: web_file.write(f'<td colspan={n_empty}>  </td>\n')
              #print(steps, nsteps, reduced_steps.index(steps[-1]), reduced_steps.index(steps[0]), '\n  ',reduced_steps)
              # Fill columns
              #web_line = f'<td colspan={nsteps}> <a href="{prepip["setup"]}"> {steps[0]} </a>'
              web_line = f'<td colspan={nsteps} '
              percent_complete = 0
              if steps[0] == 'PAT' or steps[0] == 'NANO': 
                percent_complete = prepip['completed_events']*1./chain['nevents']*100
                web_line += f"class='withBGdone' style='background-size: {percent_complete}% 100%'"
              web_line += '>'
              if steps[0] == 'LHE' or steps[0] == 'GEN':  web_line += f'<a href="{chain["fragment"]}"><span title="Fragment">F</span></a>'
              web_line += f'<a href="{prepip["setup"]}"><span title="Setup">S</span></a>'
              web_line += f'<a href="{prepip["mcm"]}"><span title="McM">M</span></a>'
              if steps[0] == 'PAT' or steps[0] == 'NANO': 
                if prepip["das"] != '':
                  web_line += f'<a href="{prepip["das"]}"><span title="DAS">D</span></a> <span title="{percent_complete:.1f}%">{percent_complete:.0f}</span></a>'
              web_line += '</td>\n'
              web_file.write(web_line)
            web_file.write('</tr>\n')
  
  
  
    web_file.write('</table>\n')
    web_file.write('</html>\n')
    web_file.close()
