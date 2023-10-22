# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import io
import logging
import json
import base64
import PyPDF2
from anyio import sleep
import azure.functions as func
import azure.durable_functions as df
#import fitz

def orchestrator_function(context: df.DurableOrchestrationContext):    
    #doc = open('C://DOCPDF//nn.pdf', 'rb')    
    #pdf_reader = PyPDF2.PdfReader(pdf_file)
     
    # doc = fitz.open('C://DOCPDF//nn.pdf') 
    # text = "" 
    # for page in doc: 
    #     text+=page.get_text() 
    # print(text) 

    
    #page_data_list = []
    task=[]
    # for page_num in range(1,len(pdf_reader.pages)+1):
    #     page = pdf_reader.pages[page_num-1]
    #     f=io.BytesIO()
    #     writter=PyPDF2.PdfWriter()
    #     writter.add_page(page)
    #     writter.write(f)
    #     f.seek(0)
    #     page_base64 = base64.b64encode(f.read()).decode('utf-8')
        
    #     fileJson={"file":page_base64,"sourceFileName":pdf_file.name,"pageNo":page_num}
        
    #     task.append(context.call_activity('ActivityTextExtractionPagesStore',fileJson))
    #     if(page_num%19==0 or page_num==len(pdf_reader.pages)):
    #        yield context.task_any(task)
    #        task=[]
    # pdf_file.close()
    
    result=yield context.call_activity('ActivityGetBatch',"")
    yield context.call_activity("ActivityUploadSearcService",result)
    return 0
    aa=context.task_all(context.call_activity('ActivityGetBatch',"1"),context.call_activity('ActivityGetBatch',"2"))
    batch=[]
    for b in aa:
        batch.append(context.call_activity("ActivityUploadSearcService",b))
    yield context.task_all(batch)
    return 0
    page_num=0
    if not context.is_replaying:
        for tsk in batch:
            page_num=page_num+1
            task.append(context.call_activity('ActivityTextExtractionPagesStore',tsk))
            if(page_num%51==0 or page_num==len(batch)):
                result = yield context.task_all(task)
                yield context.call_activity("ActivityUploadSearcService",result)
                task=[]
                    
    return 

main = df.Orchestrator.create(orchestrator_function)