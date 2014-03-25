import os
import sys
import io

import json
from posixpath import basename
from urlparse import urlsplit
import random

import concurrent.futures
import requests
from PyPDF2 import PdfFileReader, PdfFileWriter
from StringIO import StringIO

import google

def load_pdf(url,timeout):
    return requests.get(url=url, timeout=timeout, proxies={'http' : 'http://202.162.212.171:3128'}).content

def save_file(url,text,output):
    dir_path = 'pdfs/' + text[:2] + "/" + text

    try:
        os.makedirs(dir_path)
    except OSError:
        pass

    file_name = basename(urlsplit(url).path)[:-4]+".pdf"
    outputStream=io.open(os.path.join(dir_path, file_name), 'wb')
   
    output.write(outputStream)
    outputStream.close()

    file_name_txt = "urls.txt"
    with open(os.path.join(dir_path, file_name_txt), 'a') as filetext:
        filetext.write(url+'\n')

def main():
    count = 0;
    print("start:")
    print("loaded states")
    with open("ICP01.txt", 'rb') as csv_file, open("states.json", 'rb') as states_file:
        states=json.load(states_file)
        csv_file.readline()
        #['http://ofmpub.epa.gov/echo/dfr_rest_services.get_dfr?p_id=AKG370356','AKG370356']]
        for row in csv_file:
            URLS=[]

            #used only for testing
            if count > 10:
                break
            else:
                row = row.split('|')
                url = row[1]
                text = row[0].strip('\"')
                if text[:2] == "CA":
                    count+=1
                    print(text)

                    try:
                        ran=float(random.randrange(2,5))
                        print(ran)
                        #time.sleep(ran)
                        bingResult = google.search("npdes permit "+text+" filetype:pdf",pause=ran)
                    except AttributeError:
                        print("empty bing search")
                    else:
                        for url in bingResult:
                            URLS.append(url)
                        print(URLS)
                        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
                            future_to_url={executor.submit(load_pdf, url, 60): url for url in URLS}
                            for future in concurrent.futures.as_completed(future_to_url):
                                print("####################################")
                                url=future_to_url[future]
                                try:
                                    remoteFile=future.result()
                                except Exception:
                                    print("remoteFile pass")
                                    pass
                                else:
                                    output=PdfFileWriter()
                                    memoryFile=StringIO(remoteFile)
                                    try:
                                        pdfFile=PdfFileReader(memoryFile)
                                        pdfValidate=pdfFile.getPage(0).extractText().lower().replace(" ", "")
                                    except Exception:
                                        #dir_path = 'pdfs/' + text[:2] + "/" + text
                                        #try:
                                        #    os.makedirs(dir_path)
                                        #except OSError:
                                        #    pass
                                        #file_name = basename(urlsplit(url).path)[:-4]+".pdf"
                                        #with io.open(os.path.join(dir_path, file_name), 'wb') as code:
                                        #    code.write(remoteFile)
                                        print("pdfFile pass")
                                        pass
                                    else:
                                        #wastedischargerequirements == CA
                                        #
                                        if ("wastedischargerequirements" in pdfValidate
                                        and "tentative" not in pdfValidate
                                        and "-xxxx" not in pdfValidate
                                        #and "dear" not in pdfValidate
                                        and text.lower() in pdfValidate
                                        ):
                                            print("********* " + url)
                                            print("********* IS NPDES")
                                            for pageNum in xrange(pdfFile.getNumPages()):
                                                currentPage=pdfFile.getPage(pageNum)
                                                output.addPage(currentPage)
                                            
                                            try:
                                                save_file(url,text,output)
                                            except Exception:
                                                dir_path = 'pdfs/' + text[:2] + "/" + text
                                                try:
                                                    os.makedirs(dir_path)
                                                except OSError:
                                                    pass
                                                file_name = basename(urlsplit(url).path)[:-4]+".pdf"

                                                with io.open(os.path.join(dir_path, file_name), 'wb') as code:
                                                    code.write(remoteFile)
                                                
                                                file_name_txt = "urls.txt"
                                                with open(os.path.join(dir_path, file_name_txt), 'a') as filetext:
                                                    filetext.write(url+'\n')
                                            #dir_path = 'pdfs/' + text[:2] + "/" + text
                                            
                                            #try:
                                            #    os.makedirs(dir_path)
                                            #except OSError:
                                            #    pass

                                            #file_name = basename(urlsplit(url).path)[:-4]+".pdf"
                                            #outputStream=io.open(os.path.join(dir_path, file_name), 'wb')
                                            #output.write(outputStream)
                                            #outputStream.close()
                                            #print(pdfValidate)
                                        #else:
                                        #    print("NOT NPDES")
                                print("####################################")


if __name__ == '__main__':
    main()

