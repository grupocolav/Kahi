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
        source={}
        if "submission_charges" in reg.keys():
            source["submission_charges"]=reg["submission_charges"]["average_price"] if "average_price" in reg["submission_charges"].keys() else ""
            source["submission_currency"]=reg["submission_charges"]["currency"] if "currency" in reg["submission_charges"].keys() else ""
        else:
            source["submission_charges"]=""
            source["submission_currency"]=""
        if "apc" in reg.keys():
            source["apc_charges"]=reg["apc"]["average_price"] if "average_price" in reg["apc"].keys() else ""
            source["apc_currency"]=reg["apc"]["currency"] if "currency" in reg["apc"].keys() else ""
        else:
            source["apc_charges"]=""
            source["apc_currency"]=""

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