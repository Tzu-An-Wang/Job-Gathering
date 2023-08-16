import os
import json
import datetime
import pandas as pd
from serpapi import GoogleSearch

api_key = os.environ.get('OPENAI_API_KEY')
if not api_key:
    raise ValueError("API Key not found!")

def everyday_job_search_raw(api_key=api_key,job_title="data scientist",location="San Francisco"):
    job_result = []
    params = {
    "engine": "google_jobs",
    "q": job_title, 
    "location": location,
    "api_key": api_key,
    "lrad": "100",
    "start":0,
	
    "chips":"date_posted:today"
  	}
    while True:
        search = GoogleSearch(params)
        result_dict = search.get_dict()
        if 'error' in result_dict:
            break
        job_result.append(result_dict) 
        params["start"] += 10
    raw_data = json.dumps(job_result)
    path_filename = f"s3://job-search-etl/raw_data_{datetime.date.today().strftime('%d_%m_%Y')}.txt"
    with open(path_filename,"w") as file:
         file.write(raw_data)
    return job_result



def raw_data_to_csv(job_result):
    title = []
    company_name = []
    location = []
    via = []
    description = []
    job_highlights = []
    related_links = []
    extensions = []
    detected_extensions = []
    for page in job_result:
        for details in page['jobs_results']:
            title.append(details['title'])
            company_name.append(details['company_name'])
            location.append(details['location'])
            via.append(details['via'])
            description.append(details['description'])
            job_highlights.append(details['job_highlights'])
            related_links.append(details['related_links'])
            extensions.append(details['extensions'])
            detected_extensions.append(details['detected_extensions'])
    df = pd.DataFrame({'title':title,'company_name':company_name,'location':location,'via':via,'description':description,
                       'job_highlights':job_highlights,'related_links':related_links,'extensions':extensions,
                       'detected_extensions':detected_extensions})
    df.to_csv(f"s3://job-search-etl/organized_csv_{datetime.date.today().strftime('%d_%m_%Y')}.csv")
    return df