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
import uuid

import time
import random

#browser = Browser()
#browser.visit('http://echo.epa.gov/facility_search')
#browser.find_by_id('select')
#browser.select("state[]","")
logging.basicConfig(filename='example.log',level=logging.DEBUG)

def load_url(url, timeout):
    #print(url[0])
    conn = requests.get(url=url[0], timeout=timeout)# proxies={'http' : 'http://64.34.14.28:3127'})
    ran=float(random.randrange(10,30,1))/10.0
    print(ran)
    time.sleep(ran)
    #print(conn.content)
    print(url[0])
    return json.loads(conn.content, object_pairs_hook=collections.OrderedDict)

def main():
    count = 0;
    print("start:")
    with io.open("states.json", 'rb') as states_file:
        states=json.load(states_file)
    print("loaded states")
    with io.open("ICP01-other.txt", 'rb') as csv_file:
        csv_file.readline()
        URLS=[]
        #['http://ofmpub.epa.gov/echo/dfr_rest_services.get_dfr?p_id=AKG370356','AKG370356']]
        for row in csv_file:
            #if count >= 3:
            #    break
            #else:
            row = row.split('|')
            url = row[1]
            text = row[0].strip('\"')
            if text:
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
                print(data['Results']['Error']['ErrorMessage'])
                data['Results']['Error']
            except Exception:
                #######################    JSON CHARACTERISTICS    #######################
                print("NOT AN ERROR!!!!!!!!")
                print(title)
                mainjson={}
                #print("k")
                mainjson['date_added']='new Date()'

                mainjson.update(
                    {
                        "UUID":str(uuid.uuid1()),
                        "discharge_permit": {

                            "expiration_date": "",
                            "number": "",
                            "prefix": ""
                        },
                        "facility_address": {
                            "city": "",
                            "country": "United States",
                            "county": "",
                            "epa_region": -1,
                            "state_province": "",
                            "street": "",
                            "supplemental_address": "",
                            "zip_code": -1,
                            "zip_code2": -1,
                            "latitude": 0,
                            "longitude": 0
                        },
                         "facility_characteristics": {
                           
                            "discharge_point": "",
                            "epa_id": -1,
                            "facility_id": "",
                            "facility_status": "",
                            "facility_type": "",
                            "ownership_type": "",
                            "watershed": "",
                            "watershed_huc": -1,
                            "EPA_classification_type": "",
                            "naics":[],
                            "sic":[] 
                        },
                       "facility_name": "",
                        "facility_contacts": [
                            {
                                "contact_type": "",
                                "first_name": "",
                                "last_name": "",
                                "telephone": "",
                                
                            }
                        ]   
                    })
                mainjson['discharge_permit'].update({
                        "number" : title[2:],
                        "prefix" : title[:2]
                    })

                #print(mainjson)

                permitCount=0
                for permits in data['Results']['Permits']:
                    if permits['Statute'] == "CWA":
                        if permitCount==0:
                            #print(permits['SourceID'])
                            mainjson['facility_characteristics'].update({
                                    "facility_id":permits['SourceID']
                                })
                            permitCount=1
                        else:
                            mainjson['facility_characteristics'].update({
                                    "facility_id":permits['SourceID']
                                })

                    if permits['EPASystem'] == "FRS":
                        if permits['SourceID']:
                            epaid=-1
                            try:
                                int(permits['SourceID'])
                            except Exception:
                                pass
                            mainjson['facility_characteristics'].update({
                                    "epa_id" : epaid
                                })

                    if permits['Statute'] == "CWA" and permits['SourceID'] == title:

                        if permits['ExpDate']:
                            olddate = permits['ExpDate']
                            newdate = olddate[3:5] + "/" + olddate[:2]+ olddate[5:]
                            mainjson['discharge_permit'].update({
                                    "expiration_date":newdate
                                })
                        eparegion=-1
                        try:
                            int(permits['EPARegion'])
                        except Exception:
                            pass
                        mainjson['facility_address'].update({
                                "epa_region": eparegion
                            })
                        state=""
                        try:
                            state=states['States'][permits['FacilityState']]
                        except Exception:
                            state=permits['FacilityState']

                        if permits['FacilityState']:
                            mainjson['facility_address'].update({
                                'state_province': state
                            })

                        if permits['FacilityCity']:
                            mainjson['facility_address'].update({
                                    "city" : permits['FacilityCity'].title()
                            })
                        if permits['FacilityStreet'] and permits['FacilityStreet'] != "UNKNOWN":
                            mainjson['facility_address'].update({
                                    "street" : permits['FacilityStreet'].title()
                                })
                        if permits['FacilityZip'] and permits['FacilityZip'] != "00000":
                            
                            zipcode=permits['FacilityZip']

                            mainjson['facility_address'].update({
                                    "zip_code" : zipcode
                                })
                        if permits['Latitude']:
                            latitude=0
                            try:
                                latitude=float(permits['Latitude'])
                            except Exception:
                                pass
                            mainjson['facility_address'].update({
                                    "latitude" : latitude
                                })
                        if permits['Longitude']:
                            longitude=0
                            try:
                                longitude=float(permits['Longitude'])
                            except Exception:
                                pass
                            mainjson['facility_address'].update({
                                    "longitude" : longitude
                                })


                        if permits['FacilityName']:
                            mainjson.update({
                                    "facility_name":permits['FacilityName'].title()
                                })

                        if permits['FacilityStatus']:
                            mainjson['facility_characteristics'].update({
                                    "facility_status" : permits['FacilityStatus']
                                })
                        if permits['Universe']:
                            mainjson['facility_characteristics'].update({
                                    "EPA_classification_type" : permits['Universe'][:5]
                                })
                        if permits['Areas']:
                            mainjson['facility_characteristics'].update({
                                    "ownership_type" : permits['Areas']
                                })
                        if permits['SIC']:
                            if permits['SIC'].find("4952") > -1:
                                mainjson['facility_characteristics'].update({
                                        "facility_type" : "Municipal"
                                    })
                            else:
                                mainjson['facility_characteristics'].update({
                                        "facility_type" : "Industrial"
                                    })

                if data['Results']['WaterQuality']:
                    water=data['Results']['WaterQuality']['Sources'][0]
                    if water['ReceivingWaters']:
                        mainjson['facility_characteristics'].update({
                                "discharge_point" : water['ReceivingWaters'].title()
                            })
                    if water['HUC8'] and water['HUC8Name']:
                        huc8=-1
                        try:
                            huc8=int(water['HUC8'])
                        except Exception:
                            pass
                        mainjson['facility_characteristics'].update({
                                "watershed_huc" : huc8,
                                "watershed" : water['HUC8Name'].title()
                            })
                    else:
                        if water['WBD12'] and water['WBD12Name']:
                            wbd12=-1
                            try:
                                int(water['WBD12'])
                            except Exception:
                                pass
                            mainjson['facility_characteristics'].update({
                                    "watershed_huc": wbd12,
                                    "watershed" : water['WBD12Name'].title()
                                })

                sics=[]
                naicsr=[]
                if data['Results']['SIC']:
                    for temp in data['Results']['SIC']['Sources']:
                        for sic in temp['SICCodes']:
                            desc=""
                            if sic['SICDesc']:
                                desc=sic['SICDesc']

                            sicscode=-1
                            try:
                                sicscode=int(sic['SICCode'])
                            except Exception:
                                pass

                            sics.append({
                                    "SourceID": sic['SourceID'],
                                    "EPASystem": sic['EPASystem'],
                                    "sic_code": sicscode,
                                    "sic_desc": desc
                                })

                if data['Results']['NAICS']:
                    for temp in data['Results']['NAICS']['Sources']:
                        for naics in temp['NAICSCodes']:
                            desc = ""
                            if naics['NAICSDesc']:
                                desc=naics['NAICSDesc']

                            naicscode=-1
                            try:
                                naicscode=int(naics['NAICSCode'])
                            except Exception:
                                pass

                            naicsr.append({
                                "SourceID": naics['SourceID'],
                                "EPASystem": naics['EPASystem'],
                                "naics_code": naicscode,
                                "naics_desc": desc
                            })

                mainjson['facility_characteristics'].update({
                        "sic":sics,
                        "naics":naicsr
                })

                print(mainjson)

                dir_path = os.path.dirname(__file__) + '/jsons' + '/main/' + title[0:2]
                #print(dir_path)
                try:
                    os.makedirs(dir_path)
                except OSError:
                    print("OS FAIL")
                    #print(dir_path)

                file_name = title+".json"
                print(file_name)
                with io.open(os.path.join(dir_path, file_name), 'wb') as outfile:
                    json.dump(mainjson, outfile, indent=4, separators=(',', ': '))
            print("###############################################################")

if __name__ == '__main__':
    main()







