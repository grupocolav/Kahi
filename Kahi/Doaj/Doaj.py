from time import time
from datetime import datetime as dt
import iso3166

class Doaj():
    def __init__(self):
        '''
        Base class to parse Scholar data into colva standard
        '''
        pass

    def parse_source(self,reg):
        if "bibjson" in reg.keys():
            reg=reg["bibjson"]
        source={}
        source["updated"]=int(time())
        source["source_checked"]=[{"source":"doaj","ts":int(time())}]
        source["title"]=""
        source["type"]=""
        source["publisher"]=""
        source["institution"]=""
        source["institution_id"]=""
        source["external_urls"]=[]
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
        source["aliases"]=[]
        source["subjects"]=[]
        source["keywords"]=[]
        source["author_copyright"]=""
        source["license"]=[]
        source["languages"]=[]
        source["plagiarism_detection"]=""
        source["active"]=""
        source["publication_time"]=""
        source["deposit_policies"]=[]

        
        if "country" in reg.keys():
            source["country"]=reg["country"]
        if "subject" in reg.keys():
            source["subjects"]=reg["subject"]
        if "keywords" in reg.keys():
            source["keywords"]=reg["keywords"]
        if "link" in reg.keys():
            for link in reg["link"]:
                if link["type"]=="homepage":
                    source["external_urls"].append({"source":"site","url":link["url"]})
        if "language" in reg.keys():
            source["languages"]=reg["language"]
        if "title" in reg.keys():
            source["title"]=reg["title"]
        if "plagiarism_detection" in reg.keys():
            source["plagiarism_detection"]=reg["plagiarism_detection"]["detection"]
        if "institution" in reg.keys(): #The institution id is linked in the kahi class
            source["institution"]=reg["institution"]
        if "editorial_review" in reg.keys():
            source["editorial_review"]=reg["editorial_review"]["process"]
        if "deposit_policy" in reg.keys():
            source["deposit_policies"]=reg["deposit_policy"]
        if "identifier" in reg.keys():
            for idx in reg["identifier"]:
                source["serials"].append({"type":idx["type"],"value":idx["id"].replace("-","")})
        if "active" in reg.keys():
            source["active"]=reg["active"]
        if "author_copyright" in reg.keys():
            source["author_copyright"]=reg["author_copyright"]["copyright"]
        if "publisher" in reg.keys():
            source["publisher"]=reg["publisher"]
        if "publication_time" in reg.keys():
            source["publication_time"]=reg["publication_time"]
        if "license" in reg.keys():
            source["license"]=reg["license"]
        if "alternative_title" in reg.keys():
            source["aliases"].append(reg["alternative_title"])
        if "submission_charges" in reg.keys():
            if "average_price" in reg["submission_charges"].keys():
                source["submission_charges"]=reg["submission_charges"]["average_price"]
            if "currency" in reg["submission_charges"].keys():
                source["submission_currency"]=reg["submission_charges"]["currency"]
        if "apc" in reg.keys():
            if "average_price" in reg["apc"].keys():
                source["apc_charges"]=reg["apc"]["average_price"]
            if "currency" in reg["apc"].keys():
                source["apc_currency"]=reg["apc"]["currency"]


        return source
    
    def parse_one(self,register):
        """
        Transforms the raw register from DOAJ in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in DOAJ format
        
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
    
        if "bibjson" in register.keys():
            reg=register["bibjson"]
        else:
            reg=register
        return ([],
                [],
                [],
                self.parse_source(reg))