# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import os

from aiohttp import JsonPayload


def main(document: JsonPayload) -> str:
    saveData(document)
    return f"Done!"

def saveData(text):
    import pysolr
    try:
        solr = pysolr.Solr('http://localhost:8983/solr/new_core')
        #doc = {  'id': f"{os.path.basename(filename)}-{page_number}.pdf",  'text': text}
        solr.add(text)
        solr.commit()
    except Exception as ex:
        print(ex)
