import os
import re
import io
import re
import sys
import logging
import requests
import concurrent.futures
import json, collections
import string

import time
import random
#browser = Browser()
#browser.visit('http://echo.epa.gov/facility_search')
#browser.find_by_id('select')
#browser.select("state[]","")
logging.basicConfig(filename='example.log',level=logging.DEBUG)

def load_url(url, timeout):
    conn = requests.get(url=url[0], timeout=timeout)#, proxies={'http' : 'http://23.21.110.176:80'})
    ran=float(random.randrange(10,30,1))/10.0
    print(ran)
    time.sleep(ran)
    #print(conn.content)
    print(url[0])
    return json.loads(conn.content, object_pairs_hook=collections.OrderedDict)

def main():
    count = 0
    print("start:")
    with io.open("parameter_codes.json", 'rb') as json_file:
        codes=json.load(json_file)
    print("loaded codes")
    with io.open("ICP01.TXT", 'rb') as csv_file:
        csv_file.readline()
        URLS=[]
        for row in csv_file:
            #if count >= 5:
            #    break
            #else:
            row = row.split('|')
            url = row[1]
            text = row[0].strip('\"')
            if text:
                #print(text)
                #http://ofmpub.epa.gov/echo/dfr_rest_services.get_dfr?p_id=<<FACILITY_ID>>&output=JSON --outputs everything into 1 json
                URLS.append(['http://ofmpub.epa.gov/echo/dfr_rest_services.get_dfr?p_id='+text,text])
            count+=1
    print("loaded textfile")

    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
    # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
        for future in concurrent.futures.as_completed(future_to_url):
            print("###############################################################")
            url=future_to_url[future]
            title=url[1]
            url=url[0]
            try:
                data = future.result()
            except Exception:
                print("pass")
                pass
            else:
                try:
                    print(data['Results']['Error']['ErrorMessage'])
                    data['Results']['Error']
                except Exception as exc:
                    #######################    JSON VIOLATION #######################
                    #print("NOT AN ERROR!!!!!!!!")
                    print(title)

                    if(data['Results']['CWAEffluentCompliance']):
                        print("SOMETHING IS INSIDE")
                        #print(title)
                        violation={}

                        violation['date_added']='new Date()'

                        violation.update({
                            "violation_summary":{
                                "date_last_penalty" : "",
                                "qtrs_in_non_compliance" : -1,
                                "facility_name" : "",
                                "date_last_formal_action" : "",
                                "facility_id" : title,
                                "number_of_inspections" : -1,
                                "effluent_exceedances" : -1,
                                "penalty_value" : -1,
                                "formal_enforcement_actions" : -1,
                                "value_last_penalty" : -1
                            }
                        })
                        if data['Results']['FormalActions']:
	                        for actions in data['Results']['FormalActions']['Action']:
	                            if actions['Statute'] == "CWA" and actions['SourceID'] == title:
	                                last_penalty=-1
	                                try:
	                                    last_penalty=int(actions['PenaltyAmount'])
	                                except Exception:
	                                    pass
	                                olddate = actions['ActionDate']
	                                newdate = olddate[3:5] + "/" + olddate[:2]+ olddate[5:]
	                                violation['violation_summary'].update({
	                                    "date_last_penalty" : newdate,
	                                    "date_last_formal_action" : newdate,
	                                    "value_last_penalty" : last_penalty
	                                })
	                                break
	                                
                        for permits in data['Results']['Permits']:
                            if permits['Statute'] == "CWA" and permits['SourceID'] == title:
                                violation['violation_summary'].update({
                                    "facility_name" : permits['FacilityName']
                                })

                        for summary in data['Results']['EnforcementComplianceSummaries']['Summaries']:
                            if summary['Statute'] == "CWA" and permits['SourceID'] == title:
                                if summary['QtrsInNC']:
                                    qtrsNC=-1
                                    try:
                                        qtrsNC=int(summary['QtrsInNC'])
                                    except Exception:
                                        pass
                                    violation['violation_summary'].update({
                                        "qtrs_in_non_compliance" : qtrsNC
                                    })
                                if summary['Inspections']:
                                    inspection=-1
                                    try:
                                        inspection=int(summary['Inspections'])
                                    except Exception:
                                        pass
                                    violation['violation_summary'].update({
                                        "number_of_inspections" : inspection
                                    })
                                if summary['TotalPenalties']:
                                    total_penalty=-1
                                    try:
                                        total_penalty=int(summary['TotalPenalties'][1:].replace(",", ""))
                                    except Exception:
                                        pass
                                    violation['violation_summary'].update({
                                        "penalty_value" : total_penalty
                                    })
                                if summary['FormalActions']:
                                	formal_action=-1
                                	try:
                                		formal_action=int(summary['FormalActions'])
                                	except Exception:
                                		pass
                                	violation['violation_summary'].update({
                                		"formal_enforcement_actions" : formal_action
                                		})
                        inner_violation=[]
                        effluentCount=0
                        for param in data['Results']['CWAEffluentCompliance']['Sources'][0]['Parameters']:
                            for i in range(1,13):
                                if param['Qtr'+str(i)+'Value']:
                                    param_num = -1
                                    #print(codes)
                                    #print(json.dumps(codes, indent=4, separators=(',', ': ')))
                                    for code in codes:
                                        if ''.join(e for e in param['ParameterName'].lower().strip().replace("and","") if e.isalnum()) == ''.join(e for e in code['PARAMETER NAME'].lower().strip().replace("and","") if e.isalnum()):
                                            #print(codes['PARAMETER NAME'].lower().replace(" ", "").strip())
                                            param_num=code['CODE'].lstrip('0')
                                            #print(param_num)
                                            #print(''.join(e for e in param['ParameterName'].lower().strip() if e.isalnum()))

                                current=-1
                                try: 
                                    current = int(param['Qtr'+str(i)+'Value'][:-1])
                                except Exception:
                                    pass
                                paramnumber=-1
                                try:
                                    paramnumber=int(param_num)
                                except Exception:
                                    pass
                                if(current != -1):
                                    effluentCount+=1
                                    inner_violation.append(
                                            {
                                                "violation_name" : "effluent",
                                                "current" : current,
                                                "limit_units" : "PERCENT",
                                                "limit" : "",
                                                "date" : data['Results']['CWAEffluentCompliance']['Header']['Qtr'+str(i)+'End'],
                                                "parameter_number" : paramnumber,
                                                "parameter" : param['ParameterName'],
                                                "violation_description" : ""
                                            }
                                        )
                        
                        violation.update(
                            {
                                "facility_violations" : inner_violation
                            })
                        if effluentCount > 0:
                            violation['violation_summary'].update(
                                {
                                    "effluent_exceedances" : effluentCount
                                })

                        
                        #print(json.dumps(violation, indent=4, separators=(',', ': ')))
                        dir_path = os.path.dirname(__file__) + '/jsons/' + '/violations/' + title[0:2]
                        try:
                            os.makedirs(dir_path)
                        except OSError:
                            pass

                        file_name = title+".json"
                        print(file_name)
                        with io.open(os.path.join(dir_path, file_name), 'wb') as outfile:
                            json.dump(violation, outfile, indent=4, separators=(',', ': '))
                    print("###############################################################")

if __name__ == "__main__":
    main()

