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
        data["bibtex"]=""
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
        data["citations"]=[]
        data["citations_link"]=""
        data["funding_organization"]=""
        data["funding_details"]=""
        data["is_open_access"]=""
        data["open_access_status"]=""
        data["external_ids"]=[]
        data["urls"]=[]
        data["source"]=""
        data["author_count"]=""
        data["authors"]=[]
        
        if "title" in reg.keys():
            data["title"]=reg["title"]
        if "year" in reg.keys():
            year=""
            try:
                if reg["year"][-1]=="\n":
                    reg["year"]=reg["year"][:-1]
                year=int(reg["year"])
            except:
                print("Could not transform year ",reg["year"]," to int")
            data["year_published"]=year
        if "doi" in reg.keys():
            data["external_ids"].append({"source":"doi","id":reg["doi"].lower()})
        if "volume" in reg.keys():
            volume=""
            try:
                if reg["volume"][-1]=="\n":
                    reg["volume"]=reg["volume"][:-1]
                volume=int(reg["volume"])
            except:
                print("Could not transform volume ",reg["volume"]," to int")
            data["volume"]=volume
        if "issue" in reg.keys():
            issue=""
            try:
                if reg["issue"][-1]=="\n":
                    reg["issue"]=reg["issue"][:-1]
                issue=int(reg["issue"])
            except:
                print("Could not transform issue ",reg["issue"]," to int")
            data["issue"]=issue 
        if "title" in reg.keys():
            data["title"]=reg["title"]
        if "abstract" in reg.keys():
            data["abstract"]=reg["abstract"]
        if "bibtex" in reg.keys():
            data["bibtex"]=reg["bibtex"]
        data["citations_count"]=int(reg["cites"]) if "cites" in reg.keys() else 0
        if "cites_link" in reg.keys():
            data["citations_link"]=reg["cites_link"]
        if "cid" in reg.keys():
            data["external_ids"]=[{"source":"scholar","id":reg["cid"]}]

 
        return data

    def parse_authors_institutions(self,reg):
        authors=[]
        fullname_list=[]
        if "author" in reg.keys():
            if reg["author"]:
                for author in reg["author"].rstrip().split(" and "):
                    entry={}
                    entry["first_names"]=""
                    entry["national_id"]=""
                    entry["last_names"]=""
                    entry["initials"]=""
                    entry["full_name"]=""
                    entry["aliases"]=[]
                    entry["affiliations"]=[]
                    entry["keywords"]=[]
                    entry["external_ids"]=[]
                    entry["corresponding"]=False
                    entry["corresponding_address"]=""
                    entry["corresponding_email"]=""
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

    def parse_source(self,reg):
        source={}
        source["updated"]=int(time())
        source["source_checked"]=[{"source":"scholar","ts":int(time())}]
        source["title"]=""
        source["title_idx"]=""
        source["type"]=""
        source["publisher"]=""
        source["publisher_idx"]=""
        source["institution"]=""
        source["institution_id"]=""
        source["country"]=""
        source["editorial_review"]=""
        source["submission_charges"]=""
        source["submission_charges_url"]=""
        source["submmission_currency"]=""
        source["apc_charges"]=""
        source["apc_currency"]=""
        source["apc_url"]=""
        source["serials"]=[]
        source["abbreviations"]=[]
        source["subjects"]=[]
        source["keywords"]=[]
        source["languages"]=[]
        source["plagiarism_detection"]=""
        source["active"]=""
        source["publication_time"]=""
        
        if "journal" in reg.keys():
            source["title"]=reg["journal"]
        source["title_idx"]=source["title"].lower()
        if "publisher" in reg.keys():
            source["publisher"]=reg["publisher"]
        source["publisher_idx"]=source["publisher"].lower()

        return source
    
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
        authors_institutions : list
            Information of the authors in the CoLav standard format
        source : dict
           Information of the source in the CoLav standard format
        """
    
        return (self.parse_document(register),
                self.parse_authors_institutions(register),
                self.parse_source(register))