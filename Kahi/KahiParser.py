from pymongo import MongoClient
import json
from time import time
from langid import classify
from currency_converter import CurrencyConverter
from fuzzywuzzy import fuzz,process
import sys

from joblib import Parallel, delayed

from Kahi.WebOfScience import WebOfScience
from Kahi.Lens import Lens
from Kahi.Scopus import Scopus
from Kahi.Scielo import Scielo
from Kahi.Scholar import Scholar
from Kahi.Oadoi import Oadoi
from Kahi.Doaj import Doaj

class KahiParser():
    '''
    Methods that uses the parsers to merge the information from the different raw dbs into one
    single register
    TODO:
    * Create a connection to Hunabku server instead of connecting directly to the database
    * Develop the update function such as it checks which field
    * is missing in a given register already in the CoLav db
    * Right now join authors assumes authors are reported in the same order
    '''
    def __init__(self,verbose=0):

        self.verbose=0

        self.document={}
        self.authors_institutions=[]
        self.source={}

        self.currency=CurrencyConverter()

        self.wos_parser=WebOfScience.WebOfScience()

        self.lens_parser=Lens.Lens()

        self.scielo_parser=Scielo.Scielo()

        self.scopus_parser=Scopus.Scopus()

        self.scholar_parser=Scholar.Scholar()

        self.oadoi_parser=Oadoi.Oadoi()

        self.doaj_parser=Doaj.Doaj()

    def document_empty(self):
        document={}
        document["updated"]=int(time())
        document["source_checked"]=[]
        document["publication_type"]=""
        document["titles"]=[]
        document["subtitle"]=""
        document["abstract"]=""
        document["abstract_idx"]=""
        document["keywords"]=[]
        document["start_page"]=""
        document["end_page"]=""
        document["volume"]=""
        document["issue"]=""
        document["date_published"]=""
        document["year_published"]=""
        document["languages"]=""
        document["bibtex"]=""
        document["funding_organization"]=""
        document["funding_details"]=""
        document["is_open_access"]=False
        document["open_access_status"]=""
        document["external_ids"]=[]
        document["urls"]=[]
        document["source"]=""
        document["author_count"]=""
        document["authors"]=[]
        document["references_count"]=""
        document["references"]=[]
        document["citations_count"]=""
        document["citations_link"]=""
        document["citations"]=[]
        return document
    
    def author_empty(self):
        entry={}
        entry["national_id"]=""
        entry["full_name"]=""
        entry["first_names"]=""
        entry["last_names"]=""
        entry["initials"]=""
        entry["aliases"]=[]
        entry["affiliations"]=[]
        entry["branches"]=[]
        entry["keywords"]=[]
        entry["external_ids"]=[]
        entry["corresponding"]=""
        entry["corresponding_address"]=""
        entry["corresponding_email"]=""
        return entry

    def institution_empty(self):
        entry={}
        entry["name"]=""
        entry["aliases"]=[]
        entry["abbreviations"]=[]
        entry["types"]=[]
        entry["relationships"]=[]
        entry["addresses"]=[]
        entry["external_urls"]=[]
        entry["external_ids"]=[]
        return entry

    def source_empty(self):
        entry={}
        entry["source_checked"]=[]
        entry["updated"]=""
        entry["title"]=""
        entry["type"]=""
        entry["publisher"]=""
        entry["institution"]=""
        entry["country"]=""
        entry["submission_charges"]=""
        entry["submission_currency"]=""
        entry["apc_charges"]=""
        entry["apc_currency"]=""
        entry["serials"]=[]
        entry["abbreviations"]=""
        entry["subjects"]={}
        return entry

    def parse_document(self,data):
        parsed={
            "lens":None,
            "scielo":None,
            "scholar":None,
            "oadoi":None,
            "wos":None,
            "scopus":None
        }
        if data["lens"]:
            parsed["lens"]=self.lens_parser.parse_document(data["lens"])
        if data["wos"]:
            parsed["wos"]=self.wos_parser.parse_document(data["wos"])
        if data["scopus"]:
            parsed["scopus"]=self.scopus_parser.parse_document(data["scopus"])
        if data["scielo"]:
            parsed["scielo"]=self.scielo_parser.parse_document(data["scielo"])
        if data["scholar"]:
            parsed["scholar"]=self.scholar_parser.parse_document(data["scholar"])
        if data["oadoi"]:
            parsed["oadoi"]=self.oadoi_parser.parse_document(data["oadoi"])
        
        return parsed

    def parse_authors_institutions(self,data):
        parsed={
            "lens":None,
            "scielo":None,
            "scholar":None,
            "oadoi":None,
            "wos":None,
            "scopus":None
        }
        if data["lens"]:
            parsed["lens"]=self.lens_parser.parse_authors_institutions(data["lens"])
        if data["wos"]:
            parsed["wos"]=self.wos_parser.parse_authors_institutions(data["wos"])
        if data["scopus"]:
            parsed["scopus"]=self.scopus_parser.parse_authors_institutions(data["scopus"])
        if data["scielo"]:
            parsed["scielo"]=self.scielo_parser.parse_authors_institutions(data["scielo"])
        if data["scholar"]:
            parsed["scholar"]=self.scholar_parser.parse_authors_institutions(data["scholar"])
        #if data["oadoi"]:
        #    parsed["oadoi"]=self.oadoi_parser.parse_authors_institutions(data["oadoi"])
        
        return parsed

    def parse_source(self,data):
        parsed={
            "lens":None,
            "scielo":None,
            "scholar":None,
            "oadoi":None,
            "wos":None,
            "scopus":None
        }
        if data["lens"]:
            parsed["lens"]=self.lens_parser.parse_source(data["lens"])
        if data["wos"]:
            parsed["wos"]=self.wos_parser.parse_source(data["wos"])
        if data["scopus"]:
            parsed["scopus"]=self.scopus_parser.parse_source(data["scopus"])
        if data["scielo"]:
            parsed["scielo"]=self.scielo_parser.parse_source(data["scielo"])
        if data["scholar"]:
            parsed["scholar"]=self.scholar_parser.parse_source(data["scholar"])
        #if data["oadoi"]:
        #    parsed["oadoi"]=self.oadoi_parser.parse_source(data["oadoi"])

        return parsed

    def join_source(self,data):
        """
        Join the source information from the given data

        Parameters
        ----------
        data : dict
            A dict with the name of the raw database as a key
            and the register as its correspondent value
        
        Returns
        -------
        source : dict
            Aggregated source information in CoLav standard
        """
        if self.verbose==5: print("JOINING SOURCES")
        source=self.source_empty()
        source["updated"]=int(time())

        abbreviations=[]
        abbreviations_values=[]
        serials=[]
        serials_values=[]
        pissn=""
        eissn=""

        if data["scopus"]:
            source["source_checked"].append({"source":"scopus","date":source["updated"]})
            if data["scopus"]["title"]:
                source["title"]=data["scopus"]["title"]
            if data["scopus"]["publisher"]:
                source["publisher"]=data["scopus"]["publisher"]
            for abb in data["scopus"]["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
            for serial in data["scopus"]["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
        if data["scielo"]:
            source["source_checked"].append({"source":"scielo","date":source["updated"]})
            if data["scielo"]["title"]:
                source["title"]=data["scielo"]["title"]
            if data["scielo"]["publisher"]:
                source["publisher"]=data["scielo"]["publisher"]
            if data["scielo"]["type"]:
                source["type"]=data["scielo"]["type"]
            for abb in data["scielo"]["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
            for serial in data["scielo"]["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
        if data["wos"]:
            source["source_checked"].append({"source":"wos","date":source["updated"]})
            if data["wos"]["title"]:
                source["title"]=data["wos"]["title"]
            if data["wos"]["publisher"]:
                source["publisher"]=data["wos"]["publisher"]
            if data["wos"]["type"]:
                source["type"]=data["wos"]["type"]
            for abb in data["wos"]["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
            for serial in data["wos"]["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
        if data["scholar"]:
            source["source_checked"].append({"source":"scholar","date":source["updated"]})
        if data["lens"]:
            source["source_checked"].append({"source":"lens","date":source["updated"]})
            if data["lens"]["title"]:
                source["title"]=data["lens"]["title"]
            if data["lens"]["publisher"]:
                source["publisher"]=data["lens"]["publisher"]
            if data["lens"]["country"]:
                source["country"]=data["lens"]["country"]
            for abb in data["lens"]["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
            for serial in data["lens"]["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
                    if serial["type"]=="eissn": eissn=serial["value"]
                    if serial["type"]=="pissn": pissn=serial["value"]


        source["abbreviations"]=abbreviations
        source["serials"]=serials
        source["title_idx"]=source["title"].lower()
        source["publisher_idx"]=source["publisher"].lower()


        return source

    def update_doaj(self,source,doaj):
        '''
        Updates the source register in colav standard
        with the raw information from DOAJ db

        '''
        doaj=self.doaj_parser.parse_source(doaj)
        for key,value in source.items():
            if key in doaj.keys():
                if key=="serials":
                    for source_serial in source[key]:
                        found=False
                        for doaj_serial in doaj[key]:
                            if doaj_serial["value"]==source_serial["value"]:
                                found=True
                                break
                        if found==False:
                            doaj[key].append(source_serial)
                if key=="source_checked":
                    entry=source[key]
                    entry.extend(doaj[key])
                    doaj[key]=entry
                if not doaj[key] and source[key]:
                    doaj[key]=source[key]
        return doaj

    def join_document(self,data):
        """
        Join the document information from the given data

        Parameters
        ----------
        data : dict
            A dict with the name of the raw database as a key
            and the register as its correspondent value
        
        Returns
        -------
        document : dict
            Aggregated document information in CoLav standard
        """
        if self.verbose==5: print("JOINING DOCUMENTS")
        document=self.document_empty()

        if data["lens"]:
            document["source_checked"].extend(data["lens"]["source_checked"])
        if data["wos"]:
            document["source_checked"].extend(data["wos"]["source_checked"])
        if data["scielo"]:
            document["source_checked"].extend(data["scielo"]["source_checked"])
        if data["oadoi"]:
            document["source_checked"].extend(data["oadoi"]["source_checked"])
        if data["scholar"]:
            document["source_checked"].extend(data["scholar"]["source_checked"])
        if data["scopus"]:
            document["source_checked"].extend(data["scopus"]["source_checked"])

        #publication type
        if data["scielo"]:
            document["publication_type"]=data["scielo"]["publication_type"] if data["scielo"]["publication_type"] else ""
        if data["scopus"]:
            if data["scopus"]["publication_type"]:
                document["publication_type"]=data["scopus"]["publication_type"]
        if data["wos"]:
            if data["wos"]["publication_type"]:
                document["publication_type"]=data["wos"]["publication_type"]
        if data["lens"]:
            if data["lens"]["publication_type"]:
                document["publication_type"]=data["lens"]["publication_type"]

        if data["lens"]:
            if data["lens"]["titles"]:
                document["titles"].extend(data["lens"]["titles"])
        if data["scielo"]:
            if data["scielo"]["titles"]:
                for title in data["scielo"]["titles"]:
                    in_document=False
                    for doc_title in document["titles"]:
                        if title["lang"]==doc_title["lang"]:
                            in_document=True
                            break
                    if not in_document:
                        document["titles"].append(title)
        if data["wos"]:
            if data["wos"]["titles"]:
                for title in data["wos"]["titles"]:
                    in_document=False
                    for doc_title in document["titles"]:
                        if title["lang"]==doc_title["lang"]:
                            in_document=True
                            break
                    if not in_document:
                        document["titles"].append(title)
        if data["scopus"]:
            if data["scopus"]["titles"]:
                for title in data["scopus"]["titles"]:
                    in_document=False
                    for doc_title in document["titles"]:
                        if title["lang"]==doc_title["lang"]:
                            in_document=True
                            break
                    if not in_document:
                        document["titles"].append(title)

        #abstract
        if data["scopus"]:
            document["abstract"]=data["scopus"]["abstract"] if data["scopus"]["abstract"] else ""
        else:
            document["abstract"]=""
        if data["scielo"]:
            if data["scielo"]["abstract"]:
                document["abstract"]=data["scielo"]["abstract"]
        if data["wos"]:
            if data["wos"]["abstract"]:
                document["abstract"]=data["wos"]["abstract"]
        if data["lens"]:
            if data["lens"]["abstract"]:
                document["abstract"]=data["lens"]["abstract"]
        
        document["abstract_idx"]=document["abstract"].lower()

        #start page
        if data["scopus"]:
            document["start_page"]=data["scopus"]["start_page"] if data["scopus"]["start_page"] else ""
        if data["scielo"]:
            if data["scielo"]["start_page"]:
                document["start_page"]=data["scielo"]["start_page"]
        if data["wos"]:
            if data["wos"]["start_page"]:
                document["start_page"]=data["wos"]["start_page"]
        if data["lens"]:
            if data["lens"]["start_page"]:
                document["start_page"]=data["lens"]["start_page"]

        #end page
        if data["scopus"]:
            document["end_page"]=data["scopus"]["end_page"] if data["scopus"]["end_page"] else ""
        if data["scielo"]:
            if data["scielo"]["end_page"]:
                document["end_page"]=data["scielo"]["end_page"]
        if data["wos"]:
            if data["wos"]["end_page"]:
                document["end_page"]=data["wos"]["end_page"]
        if data["lens"]:
            if data["lens"]["end_page"]:
                document["end_page"]=data["lens"]["end_page"]

        #volume
        if data["scopus"]:
            document["volume"]=data["scopus"]["volume"] if data["scopus"]["volume"] else ""
        if data["scielo"]:
            if data["scielo"]["volume"]:
                document["volume"]=data["scielo"]["volume"]
        if data["wos"]:
            if data["wos"]["volume"]:
                document["volume"]=data["wos"]["volume"]
        if data["lens"]:
            if data["lens"]["volume"]:
                document["volume"]=data["lens"]["volume"]

        #issue
        if data["scopus"]:
            document["issue"]=data["scopus"]["issue"] if data["scopus"]["issue"] else ""
        if data["scielo"]:
            if data["scielo"]["issue"]:
                document["issue"]=data["scielo"]["issue"]
        if data["wos"]:
            if data["wos"]["issue"]:
                document["issue"]=data["wos"]["issue"]
        if data["lens"]:
            if data["lens"]["issue"]:
                document["issue"]=data["lens"]["issue"]

        #date published
        if data["lens"]:
            if data["lens"]["date_published"]:
                document["date_published"]=data["lens"]["date_published"]

        #year published
        if data["scopus"]:
            if data["scopus"]["year_published"]:
                document["year_published"]=data["scopus"]["year_published"]
        if data["scielo"]:
            if data["scielo"]["year_published"] and document["year_published"]=="":
                document["year_published"]=data["scielo"]["year_published"]
        if data["wos"]:
            if data["wos"]["year_published"] and document["year_published"]=="":
                document["year_published"]=data["wos"]["year_published"]
        if data["lens"]:
            if data["lens"]["year_published"]:
                document["year_published"]=data["lens"]["year_published"]

        #author count
        if data["scopus"]:
            if data["scopus"]["author_count"]:
                document["author_count"]=data["scopus"]["author_count"]
        if data["scielo"]:
            if data["scielo"]["author_count"]:
                document["author_count"]=data["scielo"]["author_count"]
        if data["wos"]:
            if data["wos"]["author_count"]:
                document["author_count"]=data["wos"]["author_count"]
        if data["lens"]:
            if data["lens"]["author_count"]:
                document["author_count"]=data["lens"]["author_count"]

        #funding organization
        if data["scopus"]:
            document["funding_organization"]=data["scopus"]["funding_organization"] if data["scopus"]["funding_organization"] else ""

        #funding details
        if data["scopus"]:
            document["funding_details"]=data["scopus"]["funding_details"] if data["scopus"]["funding_details"] else ""
 
        #external ids
        ids=[]
        ids_source=[]
        ids_id=[]
        if data["lens"]:
            if data["lens"]["external_ids"]:
                for ext in data["lens"]["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if source=="doi":
                        idx=idx.lower()
                    if not idx in ids_id and not source in ids_source:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)

        if data["wos"]:
            if data["wos"]["external_ids"]:
                for ext in data["wos"]["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if source=="doi":
                        idx=idx.lower()
                    if not idx in ids_id  and not source in ids_source:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)

        if data["scielo"]:
            if data["scielo"]["external_ids"]:
                for ext in data["scielo"]["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if source=="doi":
                        idx=idx.lower()
                    if not idx in ids_id  and not source in ids_source:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)

        if data["scopus"]:
            if data["scopus"]["external_ids"]:
                for ext in data["scopus"]["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if source=="doi":
                        idx=idx.lower()
                    if not idx in ids_id  and not source in ids_source:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)
        if data["scholar"]:
            if data["scholar"]["external_ids"]:
                for ext in data["scholar"]["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if source=="doi":
                        idx=idx.lower()
                    if not idx in ids_id and not source in ids_source:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)
        

        document["external_ids"]=ids

        #urls
        if data["scopus"]:
            if "urls" in data["scopus"].keys():
                document["urls"]=data["scopus"]["urls"]
        #MISSING URLS FROM LENS

        #keywords
        if data["scopus"]:
            if "keywords" in data["scopus"].keys():
                document["keywords"]=data["scopus"]["keywords"]

        #access type
        #FIRST OADOI
        #WOS AND SCIELO REGISTERS ALSO HAVE OA INFORMATION (NOT YET PARSED)
        document["is_open_access"]=""
        document["open_access_status"]=""
        if data["oadoi"]:
            document["is_open_access"]= data["oadoi"]["is_open_access"] if "is_open_access" in oadoi.keys() else ""
            document["open_access_status"]= data["oadoi"]["open_access_status"] if "open_access_status" in oadoi.keys() else ""
        
        #languages
        languages=[]
        if data["wos"]:
            if "languages" in data["wos"].keys():
                for lang in data["wos"]["languages"]:
                    if not lang in languages:
                        languages.append(lang)

        if data["scielo"]:
            if "languages" in data["scielo"].keys():
                for lang in data["scielo"]["languages"]:
                    if not lang in languages:
                        languages.append(lang)

        if data["scopus"]:
            if "languages" in data["scopus"].keys():
                for lang in data["scopus"]["languages"]:
                    if not lang in languages:
                        languages.append(lang)
        
        document["languages"]=languages

        #references count
        if data["scopus"]:
            document["references_count"]=data["scopus"]["references_count"] if data["scopus"]["references_count"] else ""
        if data["scielo"]:
            if data["scielo"]["references_count"]:
                document["references_count"]=data["scielo"]["references_count"]
        if data["lens"]:
            if data["lens"]["references_count"]:
                document["references_count"]=data["lens"]["references_count"]
        if data["wos"]:
            if data["wos"]["references_count"]:
                document["references_count"]=data["wos"]["references_count"]

        #citations count
        if data["scopus"]:
            document["citations_count"]=data["scopus"]["citations_count"] if data["scopus"]["citations_count"] else ""
        else:
            document["citations_count"]=""
        if data["scielo"]:
            if data["scielo"]["citations_count"]:
                document["citations_count"]=data["scielo"]["citations_count"]
        if data["lens"]:
            if data["lens"]["citations_count"]:
                document["citations_count"]=data["lens"]["citations_count"]
        if data["wos"]:
            if data["wos"]["citations_count"]:
                document["citations_count"]=data["wos"]["citations_count"]
        if data["scholar"]:
            if data["scholar"]["citations_count"]!="":
                document["citations_count"]=data["scholar"]["citations_count"]
            if data["scholar"]["citations_link"]!="":
                document["citations_link"]=data["scholar"]["citations_link"]
        
        if data["scholar"]:
            document["bibtex"]=data["scholar"]["bibtex"]
        
        self.document=document
        return document

    def find_complement(self,data,entry):
        '''
        asd
        '''
        version={}
        for author in data:
            ratio=fuzz.partial_ratio(author["full_name"].lower(),entry["full_name"].lower())
            if ratio>=80:
                version=author
                break
            elif ratio>=45:
                ratio=fuzz.token_set_ratio(author["full_name"].lower(),entry["full_name"].lower())
                if ratio>=80:
                    version=author
                    break
                elif ratio>=45:
                    ratio=fuzz.partial_token_set_ratio(author["full_name"].lower(),entry["full_name"].lower())
                    if ratio>=80:
                        version=author
                        break
        if version:
            if not entry["full_name"]:
                entry["full_name"]=version["full_name"]
            else:
                if not version["full_name"].lower() in entry["aliases"]:
                    entry["aliases"].append(version["full_name"].lower())
            if not entry["first_names"]:
                entry["first_names"]=version["first_names"]
            if not entry["last_names"]:
                entry["last_name"]=version["last_names"]
            if not entry["initials"]:
                entry["initials"]=version["initials"]
            if version["corresponding"] != "" and (entry["corresponding"]==False or entry["corresponding"]==""):
                entry["corresponding"]=version["corresponding"]
            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                entry["corresponding_email"]=version["corresponding_email"]
            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                entry["corresponding_address"]=version["corresponding_address"]
            for ext in version["external_ids"]:
                found=False
                for entry_id in entry["external_ids"]:
                    if ext["source"]== entry_id["source"]:
                        found=True
                        break
                if found==False:
                    entry["external_ids"].append(ext)
            
            for aff in version["affiliations"]:
                found=False
                for entry_aff in entry["affiliations"]:
                    ratio=fuzz.partial_ratio(aff["name"].lower(),entry_aff["name"].lower())
                    #print("partial ration between ",aff["name"],entry_aff["name"]," is ",ratio)
                    if ratio>=80:
                        found=True
                        break
                    elif ratio>=45:
                        ratio=fuzz.token_set_ratio(aff["name"].lower(),entry_aff["name"].lower())
                        #print("token set ratio between ",aff["name"],entry_aff["name"]," is ",ratio)
                        if ratio>=80:
                            found=True
                            break
                        elif ratio>=45:
                            ratio=fuzz.partial_token_set_ratio(aff["name"].lower(),entry_aff["name"].lower())
                            #print("partial token set ratio between ",aff["name"],entry_aff["name"]," is ",ratio)
                            if ratio>=90:
                                found=True
                                break
                if found==False:
                    entry["affiliations"].append(aff)

        return entry

    def join_authors_institutions(self,data):
        """
        Join authors and institutions information from the given sources

        Parameters
        ----------
        data : dict
            A dict with the name of the raw database as a key
            and the register as its correspondent value
        
        Returns
        -------
        authors : list
            Aggregated authors and its affiliations in CoLav standard
        """
        authors=[]
        author_count=0
        updated=int(time())

        if data["lens"]:
            author_count=len(data["lens"])
            for i in range(author_count):
                entry=self.author_empty()

                entry["full_name"]=data["lens"][i]["full_name"] if "full_name" in data["lens"][i].keys() else ""
                entry["first_names"]=data["lens"][i]["first_names"] if "first_names" in data["lens"][i].keys() else ""
                entry["last_names"]=data["lens"][i]["last_names"] if "last_names" in data["lens"][i].keys() else ""
                entry["initials"]=data["lens"][i]["initials"] if "initials" in data["lens"][i].keys() else ""
                entry["corresponding"]=data["lens"][i]["corresponding"] if "corresponding" in data["lens"][i].keys() else ""
                if not entry["full_name"].lower() in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"].lower())
                
                #Affiliations must be linked to the database in the child class
                #Here we simply join the data collected to make the linkage easier
                entry["affiliations"]=data["lens"][i]["affiliations"]

                #Start of wos complement to lens
                if data["wos"]:
                    entry=self.find_complement(data["wos"],entry)
                #end of wos compliment to lens
                
                #Start of scielo compliment to lens
                if data["scielo"]:
                    entry=self.find_complement(data["scielo"],entry)
                #end of scielo compliment to lens

                #Start of scopus compliment to lens
                if data["scopus"]:
                    entry=self.find_complement(data["scopus"],entry)
                #end of scopus compliment to lens

                #Start of scholar compliment to lens
                if data["scholar"]:
                    entry=self.find_complement(data["scholar"],entry)
                #end of scholar compliment to lens

                entry["updated"]=updated      
                authors.append(entry)
        
        elif data["wos"]:
            author_count=len(data["wos"])
            for i in range(author_count):
                entry=self.author_empty()

                entry["full_name"]=data["wos"][i]["full_name"] if "full_name" in data["wos"][i].keys() else ""
                entry["first_names"]=data["wos"][i]["first_names"] if "first_names" in data["wos"][i].keys() else ""
                entry["last_names"]=data["wos"][i]["last_names"] if "last_names" in data["wos"][i].keys() else ""
                entry["initials"]=data["wos"][i]["initials"] if "initials" in data["wos"][i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in data["wos"][i].keys():
                    if data["wos"][i]["corresponding"] != "":
                        entry["corresponding"]=data["wos"][i]["corresponding"]
                if "corresponding_email" in data["wos"][i].keys():
                    if data["wos"][i]["corresponding_email"] != "":
                        entry["corresponding_email"]=data["wos"][i]["corresponding_email"]
                else:
                    entry["corresponding_email"]=""
                if "corresponding_address" in data["wos"][i].keys():
                    if data["wos"][i]["corresponding_address"] != "":
                        entry["corresponding_address"]=data["wos"][i]["corresponding_address"]
                
                if data["scielo"]:
                    entry=self.find_complement(data["scielo"],entry)
                if data["scopus"]:
                    entry=self.find_complement(data["scopus"],entry)
                if data["scholar"]:
                    entry=self.find_complement(data["scholar"],entry)
                
                entry["updated"]=updated      
                authors.append(entry)

        elif data["scielo"]:
            author_count=len(data["scielo"])
            for i in range(author_count):
                entry=self.author_empty()

                entry["full_name"]=data["scielo"][i]["full_name"] if "full_name" in data["scielo"][i].keys() else ""
                entry["first_names"]=data["scielo"][i]["first_names"] if "first_names" in data["scielo"][i].keys() else ""
                entry["last_names"]=data["scielo"][i]["last_names"] if "last_names" in data["scielo"][i].keys() else ""
                entry["initials"]=data["scielo"][i]["initials"] if "initials" in data["scielo"][i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in data["scielo"][i].keys():
                    if data["scielo"][i]["corresponding"] != "":
                        entry["corresponding"]=data["scielo"][i]["corresponding"]
                if "corresponding_email" in data["scielo"][i].keys():
                    if data["scielo"][i]["corresponding_email"] != "":
                        entry["corresponding_email"]=data["scielo"][i]["corresponding_email"]
                else:
                    entry["corresponding_email"]=""
                if "corresponding_address" in data["scielo"][i].keys():
                    if data["scielo"][i]["corresponding_address"] != "":
                        entry["corresponding_address"]=data["scielo"][i]["corresponding_address"]
                
                if data["scopus"]:
                    entry=self.find_complement(data["scopus"],entry)
                if data["scholar"]:
                    entry=self.find_complement(data["scholar"],entry)
            
        elif data["scopus"]:
            author_count=len(data["scopus"])
            #print("Procesing ",author_count," authors")
            for i in range(author_count):
                entry=self.author_empty()

                entry["full_name"]=data["scopus"][i]["full_name"] if "full_name" in data["scopus"][i].keys() else ""
                entry["first_names"]=data["scopus"][i]["first_names"] if "first_names" in data["scopus"][i].keys() else ""
                entry["last_names"]=data["scopus"][i]["last_names"] if "last_names" in data["scopus"][i].keys() else ""
                entry["initials"]=data["scopus"][i]["initials"] if "initials" in data["scopus"][i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in data["scopus"][i].keys():
                    if data["scopus"][i]["corresponding"] != "":
                        entry["corresponding"]=data["scopus"][i]["corresponding"]
                if "corresponding_email" in data["scopus"][i].keys():
                    if data["scopus"][i]["corresponding_email"] != "":
                        entry["corresponding_email"]=data["scopus"][i]["corresponding_email"]
                else:
                    entry["corresponding_email"]=""
                if "corresponding_address" in data["scopus"][i].keys():
                    if data["scopus"][i]["corresponding_address"] != "":
                        entry["corresponding_address"]=data["scopus"][i]["corresponding_address"]
                
                if data["scholar"]:
                    entry=self.find_complement(data["scholar"],entry)

        return authors