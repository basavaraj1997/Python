# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import io
import logging
import base64
import math
import os
import PyPDF2


def main(name: str) -> []:
    pdf_file = open('C://DOCPDF//cc.pdf', 'rb')    
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    start=1
    end=len(pdf_reader.pages)/2
    if(name=="2"):
        start=len(pdf_reader.pages)/2
        end=math.floor(len(pdf_reader.pages))
    else:
        start=1
        end=len(pdf_reader.pages)/2
            
    page_data_list = []
    task=[]
    #for page_num in range(start,end):
    for page_num in range(1,len(pdf_reader.pages)+1):

        page = pdf_reader.pages[page_num-1]
        f=io.BytesIO()
        writter=PyPDF2.PdfWriter()
        writter.add_page(page)
        writter.write(f)
        f.seek(0)
        page_text = page.extract_text()
        fileJson={"text":page_text,"sourceFileName":pdf_file.name,"pageNo":page_num}
        task.append(fileJson)
        output_folder='C://DOCPDF//pages/'  
        os.makedirs(output_folder, exist_ok=True)  
        output_pdf_path = os.path.join(output_folder, f"{os.path.basename(pdf_file.name)}-{page_num}.pdf")
        with open(output_pdf_path, "wb") as output_pdf:
            writter.write(output_pdf)
        logging.info("****"+pdf_file.name+'********'+str(page_num))
    return task

