from time import time
from datetime import datetime as dt
import iso3166

class Scholar():
    def __init__(self):
        '''
        Base class to parse Scholar data into colva standard
        '''
        pass

    def parse_document(self,reg):
        data={}
        data["citations_count"]=int(reg["cites"]) if "cites" in reg.keys() else 0
        data["citations_link"]=int(reg["cites_link"]) if "cites" in reg.keys() else ""

        return data
    
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
                [],
                [],
                [])