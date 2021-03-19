from time import time
from datetime import datetime as dt
import iso3166

class Oadoi():
    def __init__(self):
        '''
        Base class to parse Scholar data into colva standard
        '''
        pass

    def parse_document(self,reg):
        data={}
        data["updated"]=int(time())
        data["source_checked"]=[{"source":"oadoi","ts":int(time())}]
        data["publication_type"]=""
        data["titles"]=[]
        data["subtitle"]=""
        data["abstract"]=""
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
        data["funding_organization"]=""
        data["is_open_access"]=""
        data["open_access_status"]=""
        data["external_ids"]=[]
        data["urls"]=[]
        data["source"]=""
        data["author_count"]=""
        data["authors"]=[]
        data["is_open_access"]=reg["is_oa"] if "is_oa" in reg.keys() else 0
        data["open_access_status"]=reg["oa_status"] if "oa_status" in reg.keys() else ""

        return data
    
    def parse_one(self,register):
        """
        Transforms the raw register from OADOI in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in OADOI format
        
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
                [],
                [],
                [])