# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import json
import logging
import base64
import os
import io
import uuid
import PyPDF2
from aiohttp import JsonPayload
from qdrant_client import models, QdrantClient
# from sentence_transformers import SentenceTransformer
# encoder = SentenceTransformer('all-MiniLM-L6-v2')
import datetime
import openai
def main(page: JsonPayload) -> str:
    
    filePage=page["file"]
    fileName=page["sourceFileName"]
    pageNo=page["pageNo"]
    PageSave(filePage,fileName,pageNo)
    logging.info(f'filename:{pageNo}'+fileName)
    print(f'filename:{pageNo}'+fileName)
    return 'Success'


def PageSave(filePage, filename, page_number):
    ff=io.BytesIO()
    pdf_writer = PyPDF2.PdfWriter()
    bytes=base64.b64decode(filePage)
    ff.write(bytes)
    ff.seek(0)
    pdf_writer.write(ff)
    new_ff = io.BytesIO(bytes)
    output_folder='C://DOCPDF//pages/'  
    os.makedirs(output_folder, exist_ok=True)  
    output_pdf_path = os.path.join(output_folder, f"{os.path.basename(filename)}-{page_number}.pdf")
    with open(output_pdf_path, "wb") as output_pdf:
        pdf_writer.write(output_pdf)
    new_ff.seek(0)
    readr=PyPDF2.PdfReader(new_ff)
    readr.read(new_ff)
    text = readr.pages[0].extract_text()
    saveData(text,filename,page_number)
    #saveDataQdrant(text,filename,page_number)
def saveData(text,filename,page_number):
    import pysolr
    try:
        solr = pysolr.Solr('http://localhost:8983/solr/new_core')
        doc = {  'id': f"{os.path.basename(filename)}-{page_number}.pdf",  'text': text}
        solr.add(doc)
        solr.commit()
    except Exception as ex:
        print(ex)


def saveDataQdrant(documents,filename,page_number):
    # import os
    # import openai
    # openai.api_key=""
    qdrant= QdrantClient(url="https://77cdc93b-7d74-4e40-8d8b-9ba1702255f6.us-east4-0.gcp.cloud.qdrant.io",api_key="R7dmGJSOIkSiMVEsh_5o6IsBOhjuN8kd5cZ9sQLeGMPrujMflT3kLg") 
    

    coll = qdrant.get_collections()
    isCollectionexist=False
    logging.info('****************-------------------****************-----------')
    logging.info(coll)
    for collectionName in coll.collections:
        logging.info('cccccccccccccccc'+str(collectionName))
        if(collectionName.name=="my_books"):
            isCollectionexist=True
            break
    if isCollectionexist==False:
        qdrant.recreate_collection(
        collection_name="my_books",
        vectors_config=models.VectorParams(
        #size=encoder.get_sentence_embedding_dimension(),
        size=1536,
        distance=models.Distance.COSINE ))
        logging.info('Collection create on Qdrant Search service')    
    openai.api_key ="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"
    response = openai.Embedding.create(
    input=documents,
    model="text-embedding-ada-002")
    embeddings = response['data'][0]['embedding']
    idx=uuid.UUID("123e4567-e89b-12d3-a456-ab12abd13db1")
    qdrant.upload_records(
	collection_name="my_books",
	records=[
		models.Record(
			id=str(uuid.uuid5(idx,f"{os.path.basename(filename)}-{page_number}.pdf")),
			#vector=encoder.encode(documents).tolist(),
            vector=embeddings,
			payload={"fileName":f"{os.path.basename(filename)}-{page_number}.pdf","text":documents,"ProcssTime":datetime.datetime.now().strftime("%H:%M:%S")}
		) 
	])
