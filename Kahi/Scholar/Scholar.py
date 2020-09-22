from time import time
from datetime import datetime as dt
import iso3166
from fuzzywuzzy import fuzz

class Scholar():
    def __init__(self):
        '''
        Base class to parse Scholar data into colva standard
        '''
        pass

    def parse_document(self,reg):
        data={}
        data["updated"]=int(time())
        data["source_checked"]=[{"source":"scholar","ts":int(time())}]
        data["publication_type"]=""
        data["titles"]=[]
        data["subtitle"]=""
        data["abstract"]=""
        data["abstract_idx"]=""
        data["keywords"]=[]
        data["start_page"]=""
        data["end_page"]=""
        data["volume"]=""
        data["issue"]=""
        data["date_published"]=""
        data["year_published"]=""
        data["languages"]=[]
        data["references_count"]=""
        data["references"]=[]
        data["citations_count"]=""
        data["citations_link"]=""
        data["citations"]=[]
        data["funding_details"]=""
        data["is_open_access"]=""
        data["access_status"]=""
        data["external_ids"]=[]
        data["urls"]=[]
        data["source"]=""
        data["author_count"]=""
        data["authors"]=[]
        
        data["citations_count"]=int(reg["cites"]) if "cites" in reg.keys() else 0
        data["citations_link"]=reg["cites_link"] if "cites_link" in reg.keys() else ""
        data["external_ids"]=[{"source":"scholar","id":reg["cid"]}] if "cid" in reg.keys() else []

 
        return data

    def parse_authors(self,reg):
        authors=[]
        fullname_list=[]
        if "author" in reg.keys():
            if reg["author"]:
                for author in reg["author"].rstrip().split(" and "):
                    entry={}
                    entry["national_id"]=""
                    entry["first_names"]=""
                    entry["last_names"]=""
                    entry["aliases"]=[]
                    entry["external_ids"]=[]
                    names_list=author.split(", ")
                    if len(names_list)>0: entry["last_names"]=names_list[0]
                    if len(names_list)>1: entry["first_names"]=names_list[1]
                    entry["full_name"]=entry["first_names"]+" "+entry["last_names"]
                    entry["initials"]=entry["first_names"]
                    
                    authors.append(entry)
                    fullname_list.append(entry["full_name"])
        
        if "profiles" in reg.keys():
            if reg["profiles"]:
                for name in reg["profiles"].keys():
                    for i,author in enumerate(fullname_list): 
                        if fuzz.partial_ratio(name,author)>0.9:
                            authors[i]["external_ids"].append({"source":"scholar","value":reg["profiles"][name]})
                            break
   
        return authors
    
    def parse_one(self,register):
        """
        Transforms the raw register from Scholar in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Scholar format
        
        Returns
        -------
        document : dict
            Information of the document in the CoLav standard format
        authors : list
            Information of the authors in the CoLav standard format
        institutions : list
            Information of the institutions in the CoLav standard format
        source : dict
           Information of the source in the CoLav standard format
        """
    
        return (self.parse_document(register),
                self.parse_authors(register),
                [],
                [])