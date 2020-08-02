from pymongo import MongoClient
import json
from time import time
from langid import classify
from currency_converter import CurrencyConverter
from fuzzywuzzy import fuzz,process
import requests
import urllib.parse

from joblib import Parallel, delayed

from Kahi.KahiBase import KahiBase

from Kahi.WebOfScience import WebOfScience
from Kahi.Lens import Lens
from Kahi.Scopus import Scopus
from Kahi.Scielo import Scielo
from Kahi.Scholar import Scholar
from Kahi.Oadoi import Oadoi
from Kahi.Doaj import Doaj

class Kahi(KahiBase):
    '''
    Base orquestration class from each source database to the corresponding parser and back to the colav database
    TODO:
    * Create a connection to Hunabku server instead of connecting directly to the database
    * Develop the update function such as it checks which field
    * is missing in a given register already in the CoLav db
    * Right now join authors assumes authors are reported in the same order
    * Support for multiple institutions related to one author
    * Finish parsing od OADOI, scholar and DOAJ
    '''
    def __init__(self,dbserver_url="localhost",port=27017,colav_db="colav",config_file='etc/db.json',verbose=0):
        super().__init__(dbserver_url,port,colav_db,verbose=verbose)
        #if config_file:
        #    with open(config_file) as f:
        #        self.raw_dbs=json.reads(f)
        # Change this to read a config file and load the available raw dbs

        self.currecy=CurrencyConverter()

        self.colavdb=self.client["colav_better"]

        self.griddb=self.client["grid_colav"]

        self.wosdb=self.client["wos"]
        self.wos_parser=WebOfScience.WebOfScience()

        self.lensdb=self.client["lens"]
        self.lens_parser=Lens.Lens()

        self.scielodb=self.client["scielo"]
        self.scielo_parser=Scielo.Scielo()

        self.scopusdb=self.client["scopus"]
        self.scopus_parser=Scopus.Scopus()

        self.scholardb=self.client["scholar"]
        self.scholar_parser=Scholar.Scholar()

        self.oadoidb=self.client["oadoi"]
        self.oadoi_parser=Oadoi.Oadoi()

        self.doajdb=self.client["doaj"]
        self.doaj_parser=Doaj.Doaj()

    def join_document(self,scholar=None,scopus=None,scielo=None,wos=None,lens=None,oadoi=None):
        """
        Join the document information from the given sources

        Parameters
        ----------
        scholar : dict
            Document information from google scholar
        scopus : dict
            Document information from scopus
        scielo : dict
            Document information from scielo
        wos : dict
            Document information from web of science
        lens : dict
            Document information from lens
        
        Returns
        -------
        document : dict
            Aggregated document information in CoLav standard
        """
        if self.verbose==5: print("JOINING DOCUMENTS")
        document={}
        document["updated"]=int(time())

        #publication type
        if scielo:
            document["publication_type"]=scielo["publication_type"] if scielo["publication_type"] else ""
        if scopus:
            if scopus["publication_type"]:
                document["publication_type"]=scopus["publication_type"]
        if wos:
            if wos["publication_type"]:
                document["publication_type"]=wos["publication_type"]
        if lens:
            if lens["publication_type"]:
                document["publication_type"]=lens["publication_type"]

        #titles
        document["titles"]=[]
        titles=[]
        titles_lang=[]
        titles_idx=[]

        if lens:
            if lens["title"]:
                title=lens["title"]
                lang=classify(title)
                if not lang[0] in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang[0])
                    titles_idx.append(title.lower())
        if scielo:
            if scielo["title"]:
                title=scielo["title"]
                lang=classify(title)
                if not lang[0] in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang[0])
                    titles_idx.append(title.lower())
        if wos:
            if wos["title"]:
                title=wos["title"]
                lang=classify(title)
                if not lang[0] in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang[0])
                    titles_idx.append(title.lower())
        if scopus:
            if scopus["title"]:
                title=scopus["title"]
                lang=classify(title)
                if not lang[0] in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang[0])
                    titles_idx.append(title.lower())

        for idx,title in enumerate(titles): 
            document["titles"].append({"title":title,"lang":titles_lang[idx],"title_idx":titles_idx[idx]})

        #abstract
        if scopus:
            document["abstract"]=scopus["abstract"] if scopus["abstract"] else ""
        else:
            document["abstract"]=""
        if scielo:
            if scielo["abstract"]:
                document["abstract"]=scielo["abstract"]
        if wos:
            if wos["abstract"]:
                document["abstract"]=wos["abstract"]
        if lens:
            if lens["abstract"]:
                document["abstract"]=lens["abstract"]
        
        document["abstract_idx"]=document["abstract"].lower()

        #start page
        if scopus:
            document["start_page"]=scopus["start_page"] if scopus["start_page"] else ""
        if scielo:
            if scielo["start_page"]:
                document["start_page"]=scielo["start_page"]
        if wos:
            if wos["start_page"]:
                document["start_page"]=wos["start_page"]
        if lens:
            if lens["start_page"]:
                document["start_page"]=lens["start_page"]

        #end page
        if scopus:
            document["end_page"]=scopus["end_page"] if scopus["end_page"] else ""
        if scielo:
            if scielo["end_page"]:
                document["end_page"]=scielo["end_page"]
        if wos:
            if wos["end_page"]:
                document["end_page"]=wos["end_page"]
        if lens:
            if lens["end_page"]:
                document["end_page"]=lens["end_page"]

        #volume
        if scopus:
            document["volume"]=scopus["volume"] if scopus["volume"] else ""
        if scielo:
            if scielo["volume"]:
                document["volume"]=scielo["volume"]
        if wos:
            if wos["volume"]:
                document["volume"]=wos["volume"]
        if lens:
            if lens["volume"]:
                document["volume"]=lens["volume"]

        #issue
        if scopus:
            document["issue"]=scopus["issue"] if scopus["issue"] else ""
        if scielo:
            if scielo["issue"]:
                document["issue"]=scielo["issue"]
        if wos:
            if wos["issue"]:
                document["issue"]=wos["issue"]
        if lens:
            if lens["issue"]:
                document["issue"]=lens["issue"]

        #date published
        if lens:
            if lens["date_published"]:
                document["date_published"]=lens["date_published"]

        #year published
        document["year_published"]=""
        if scopus:
            if scopus["year_published"]:
                document["year_published"]=scopus["year_published"]
        if scielo:
            if scielo["year_published"] and document["year_published"]=="":
                document["year_published"]=scielo["year_published"]
        if wos:
            if wos["year_published"] and document["year_published"]=="":
                document["year_published"]=wos["year_published"]
        if lens:
            if lens["year_published"]:
                document["year_published"]=lens["year_published"]

        #author count
        document["author_count"]=""
        if scopus:
            if scopus["author_count"]:
                document["author_count"]=scopus["author_count"]
        if scielo:
            if scielo["author_count"]:
                document["author_count"]=scielo["author_count"]
        if wos:
            if wos["author_count"]:
                document["author_count"]=wos["author_count"]
        if lens:
            if lens["author_count"]:
                document["author_count"]=lens["author_count"]

        #funding organization
        if scopus:
            document["funding_organization"]=scopus["funding_organization"] if scopus["funding_organization"] else ""

        #funding details
        if scopus:
            document["funding_details"]=scopus["funding_details"] if scopus["funding_details"] else ""
 
        #external ids
        ids=[]
        ids_source=[]
        ids_id=[]
        if lens:
            if lens["external_ids"]:
                for ext in lens["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if not idx in ids_id:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)

        if wos:
            if wos["external_ids"]:
                for ext in wos["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if not idx in ids_id:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)

        if scielo:
            if scielo["external_ids"]:
                for ext in scielo["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if not idx in ids_id:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)

        if scopus:
            if scopus["external_ids"]:
                for ext in scopus["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if not idx in ids_id:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)
        if scholar:
            if scholar["external_ids"]:
                for ext in scholar["external_ids"]:
                    source=ext["source"]
                    idx=ext["id"]
                    if not idx in ids_id:
                        ids.append(ext)
                        ids_source.append(source)
                        ids_id.append(idx)
        

        document["external_ids"]=ids

        #urls
        if scopus:
            if "urls" in scopus.keys():
                document["urls"]=scopus["urls"]
        #MISSING URLS FROM LENS

        #keywords
        if scopus:
            if "keywords" in scopus.keys():
                document["keywords"]=scopus["keywords"]

        #access type
        #FIRST OADOI
        #WOS AND SCIELO REGISTERS ALSO HAVE OA INFORMATION (NOT YET PARSED)
        document["is_open_access"]=""
        document["open_access_status"]=""
        if oadoi:
            document["is_open_access"]= oadoi["is_open_access"] if "is_open_access" in oadoi.keys() else ""
            document["open_access_status"]= oadoi["open_access_status"] if "open_access_status" in oadoi.keys() else ""
        
        #languages
        languages=[]
        if wos:
            if "languages" in wos.keys():
                for lang in wos["languages"]:
                    if not lang in languages:
                        languages.append(lang)

        if scielo:
            if "languages" in scielo.keys():
                for lang in scielo["languages"]:
                    if not lang in languages:
                        languages.append(lang)

        if scopus:
            if "languages" in scopus.keys():
                for lang in scopus["languages"]:
                    if not lang in languages:
                        languages.append(lang)
        
        document["languages"]=languages

        #references count
        if scopus:
            document["references_count"]=scopus["references_count"] if scopus["references_count"] else ""
        if scielo:
            if scielo["references_count"]:
                document["references_count"]=scielo["references_count"]
        if lens:
            if lens["references_count"]:
                document["references_count"]=lens["references_count"]
        if wos:
            if wos["references_count"]:
                document["references_count"]=wos["references_count"]

        #citations count
        if scopus:
            document["citations_count"]=scopus["citations_count"] if scopus["citations_count"] else ""
        else:
            document["citations_count"]=""
        if scielo:
            if scielo["citations_count"]:
                document["citations_count"]=scielo["citations_count"]
        if lens:
            if lens["citations_count"]:
                document["citations_count"]=lens["citations_count"]
        if wos:
            if wos["citations_count"]:
                document["citations_count"]=wos["citations_count"]
        if scholar:
            if scholar["citations_count"]!="":
                document["citations_count"]=scholar["citations_count"]
            if scholar["citations_link"]!="":
                document["citations_link"]=scholar["citations_link"]
            else:
                document["citations_link"]=""
        
        
        return document

    def join_authors(self,scholar=None,scopus=None,scielo=None,wos=None,lens=None):
        """
        Join authors information from the given sources

        Parameters
        ----------
        scholar : list
            Authors information from google scholar
        scopus : list
            Authors information from scopus
        scielo : list
            Authors information from scielo
        wos : list
            Authors information from web of science
        lens : list
            Authors information from lens
        
        Returns
        -------
        authors : list
            Aggregated authors information in CoLav standard
        """
        if self.verbose==5: print("JOINING AUTHORS")
        authors=[]
        author_count=0
        updated=int(time())
        if lens:
            author_count=len(lens)
            print("Procesing ",author_count," authors")
            for i in range(author_count):
                entry={}
                entry["aliases"]=[]
                entry["external_ids"]=[]
                entry["corresponding"]=""
                entry["corresponding_address"]=""
                entry["corresponding_email"]=""

                entry["full_name"]=lens[i]["full_name"] if "full_name" in lens[i].keys() else ""
                entry["first_names"]=lens[i]["first_names"] if "first_names" in lens[i].keys() else ""
                entry["last_names"]=lens[i]["last_names"] if "last_names" in lens[i].keys() else ""
                entry["initials"]=lens[i]["initials"] if "initials" in lens[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])

                if wos:
                    #search the right author since it is not guaranteed the are in the same order
                    version={}
                    for author in wos:
                        #print("COMPARING ",author["full_name"]," WITH ",lens[i]["full_name"])
                        #print("partial ratio: ",fuzz.partial_ratio(author["full_name"],lens[i]["full_name"]))
                        #print("token set ratio: ",fuzz.token_set_ratio(author["full_name"],lens[i]["full_name"]))
                        #print("partial token set ratio: ",fuzz.partial_token_set_ratio(author["full_name"],lens[i]["full_name"]))
                        ratio=fuzz.partial_ratio(author["full_name"],lens[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],lens[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],lens[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        print("WOS VERSION")
                        print(version)
                        if not entry["full_name"]:
                            entry["full_name"]=version["full_name"]
                        else:
                            if not version["full_name"] in entry["aliases"]:
                                entry["aliases"].append(version["full_name"])
                        if not entry["first_names"]:
                            entry["first_names"]=version["first_names"]
                        if not entry["last_names"]:
                            entry["last_name"]=version["last_names"]
                        if not entry["initials"]:
                            entry["initials"]=version["initials"]
                        if "corresponding" in version.keys():
                            if version["corresponding"] != "" and entry["corresponding"]=="":
                                entry["corresponding"]=version["corresponding"]
                        if "corresponding_email" in version.keys():
                            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                                entry["corresponding_email"]=version["corresponding_email"]
                        if "corresponding_address" in version.keys():
                            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                                entry["corresponding_address"]=version["corresponding_address"]
                        print(entry)
                if scielo:
                    #search the right author since it is not guaranteed the are in the same order
                    version={}
                    for author in scielo:
                        ratio=fuzz.partial_ratio(author["full_name"],lens[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],lens[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],lens[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if not entry["full_name"]:
                            entry["full_name"]=version["full_name"]
                        else:
                            if not version["full_name"] in entry["aliases"]:
                                entry["aliases"].append(version["full_name"])
                        if not entry["first_names"]:
                            entry["first_names"]=version["first_names"]
                        if not entry["last_names"]:
                            entry["last_name"]=version["last_names"]
                        if not entry["initials"]:
                            entry["initials"]=version["initials"]
                        if "corresponding" in version.keys():
                            if version["corresponding"] != "" and entry["corresponding"]=="":
                                entry["corresponding"]=version["corresponding"]
                        if "corresponding_email" in version.keys():
                            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                                entry["corresponding_email"]=version["corresponding_email"]
                        if "corresponding_address" in version.keys():
                            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                                entry["corresponding_address"]=version["corresponding_address"]
                if scopus:
                    #search the right author since it is not guaranteed the are in the same order
                    version={}
                    for author in scopus:
                        #print("COMPARING ",author["full_name"]," WITH ",lens[i]["full_name"])
                        #print("partial ratio: ",fuzz.partial_ratio(author["full_name"],lens[i]["full_name"]))
                        #print("token set ratio: ",fuzz.token_set_ratio(author["full_name"],lens[i]["full_name"]))
                        #print("partial token set ratio: ",fuzz.partial_token_set_ratio(author["full_name"],lens[i]["full_name"]))
                        ratio=fuzz.partial_ratio(author["full_name"],lens[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],lens[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],lens[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                                else:
                                    print("Could not find associated author in scopus: ",lens[i]["full_name"])
                    if version:
                        #print("VERSION SCOPUS")
                        #print(version)
                        if not entry["full_name"]:
                            entry["full_name"]=version["full_name"]
                        else:
                            if not version["full_name"] in entry["aliases"]:
                                entry["aliases"].append(version["full_name"])
                        if not entry["first_names"]:
                            entry["first_names"]=version["first_names"]
                        if not entry["last_names"]:
                            entry["last_name"]=version["last_names"]
                        if not entry["initials"]:
                            entry["initials"]=version["initials"]
                        if "corresponding" in version.keys():
                            if version["corresponding"] != "" and entry["corresponding"]=="":
                                entry["corresponding"]=version["corresponding"]
                        if "corresponding_email" in version.keys():
                            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                                entry["corresponding_email"]=version["corresponding_email"]
                        if "corresponding_address" in version.keys():
                            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                                entry["corresponding_address"]=version["corresponding_address"]
                        entry["external_ids"]=version["external_ids"] if "external_ids" in version.keys() else []
                #end of scopus update
                if scholar:
                    version={}
                    for author in scholar:
                        ratio=fuzz.partial_ratio(author["full_name"],lens[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],lens[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],lens[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if version["external_ids"]:
                            entry["external_ids"].extend(version["external_ids"])
                        if not version["full_name"] in entry["full_name"]:
                            entry["aliases"].append(version["full_name"])

                entry["updated"]=updated      
                authors.append(entry)
        
        #End of lens section

        elif wos:
            author_count=len(wos)
            print("Procesing ",author_count," authors")
            for i in range(author_count):
                entry={}
                entry["aliases"]=[]
                entry["external_ids"]=[]
                entry["corresponding"]=""
                entry["corresponding_address"]=""
                entry["corresponding_email"]=""

                entry["full_name"]=wos[i]["full_name"] if "full_name" in wos[i].keys() else ""
                entry["first_names"]=wos[i]["first_names"] if "first_names" in wos[i].keys() else ""
                entry["last_names"]=wos[i]["last_names"] if "last_names" in wos[i].keys() else ""
                entry["initials"]=wos[i]["initials"] if "initials" in wos[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in wos[i].keys():
                    if wos[i]["corresponding"] != "":
                        entry["corresponding"]=wos[i]["corresponding"]
                if "corresponding_email" in wos[i].keys():
                    if wos[i]["corresponding_email"] != "":
                        entry["corresponding_email"]=wos[i]["corresponding_email"]
                else:
                    entry["corresponding_email"]=""
                if "corresponding_address" in wos[i].keys():
                    if wos[i]["corresponding_address"] != "":
                        entry["corresponding_address"]=wos[i]["corresponding_address"]

                if scielo:
                    #search the right author since it is not guaranteed the are in the same order
                    version={}
                    for author in scielo:
                        ratio=fuzz.partial_ratio(author["full_name"],wos[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],wos[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],wos[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if not entry["full_name"]:
                            entry["full_name"]=version["full_name"]
                        else:
                            if not version["full_name"] in entry["aliases"]:
                                entry["aliases"].append(version["full_name"])
                        if not entry["first_names"]:
                            entry["first_names"]=version["first_names"]
                        if not entry["last_names"]:
                            entry["last_name"]=version["last_names"]
                        if not entry["initials"]:
                            entry["initials"]=version["initials"]
                        if "corresponding" in version.keys():
                            if version["corresponding"] != "" and entry["corresponding"]=="":
                                entry["corresponding"]=version["corresponding"]
                        if "corresponding_email" in version.keys():
                            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                                entry["corresponding_email"]=version["corresponding_email"]
                        if "corresponding_address" in version.keys():
                            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                                entry["corresponding_address"]=version["corresponding_address"]
                if scopus:
                    #search the right author since it is not guaranteed the are in the same order
                    version={}
                    for author in scopus:
                        ratio=fuzz.partial_ratio(author["full_name"],wos[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],wos[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],wos[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if not entry["full_name"]:
                            entry["full_name"]=version["full_name"]
                        else:
                            if not version["full_name"] in entry["aliases"]:
                                entry["aliases"].append(version["full_name"])
                        if not entry["first_names"]:
                            entry["first_names"]=version["first_names"]
                        if not entry["last_names"]:
                            entry["last_name"]=version["last_names"]
                        if not entry["initials"]:
                            entry["initials"]=version["initials"]
                        if "corresponding" in version.keys():
                            if version["corresponding"] != "" and entry["corresponding"]=="":
                                entry["corresponding"]=version["corresponding"]
                        if "corresponding_email" in version.keys():
                            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                                entry["corresponding_email"]=version["corresponding_email"]
                        if "corresponding_address" in version.keys():
                            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                                entry["corresponding_address"]=version["corresponding_address"]
                        entry["external_ids"]=version["external_ids"] if "external_ids" in version.keys() else []
                #end of scopus update
                if scholar:
                    version={}
                    for author in scholar:
                        ratio=fuzz.partial_ratio(author["full_name"],wos[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],wos[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],wos[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if version["external_ids"]:
                            entry["external_ids"].extend(version["external_ids"])
                        if not version["full_name"] in entry["full_name"]:
                            entry["aliases"].append(version["full_name"])

                entry["updated"]=updated      
                authors.append(entry)
        
        #End of wos section
        
        elif scielo:
            author_count=len(scielo)
            print("Procesing ",author_count," authors")
            for i in range(author_count):
                entry={}
                entry["aliases"]=[]
                entry["external_ids"]=[]
                entry["corresponding"]=""
                entry["corresponding_address"]=""
                entry["corresponding_email"]=""

                entry["full_name"]=scielo[i]["full_name"] if "full_name" in scielo[i].keys() else ""
                entry["first_names"]=scielo[i]["first_names"] if "first_names" in scielo[i].keys() else ""
                entry["last_names"]=scielo[i]["last_names"] if "last_names" in scielo[i].keys() else ""
                entry["initials"]=scielo[i]["initials"] if "initials" in scielo[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in scielo[i].keys():
                    if scielo[i]["corresponding"] != "":
                        entry["corresponding"]=scielo[i]["corresponding"]
                if "corresponding_email" in scielo[i].keys():
                    if scielo[i]["corresponding_email"] != "":
                        entry["corresponding_email"]=scielo[i]["corresponding_email"]
                else:
                    entry["corresponding_email"]=""
                if "corresponding_address" in scielo[i].keys():
                    if scielo[i]["corresponding_address"] != "":
                        entry["corresponding_address"]=scielo[i]["corresponding_address"]

                if scopus:
                    #search the right author since it is not guaranteed the are in the same order
                    version={}
                    for author in scopus:
                        ratio=fuzz.partial_ratio(author["full_name"],scielo[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],scielo[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],scielo[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if not entry["full_name"]:
                            entry["full_name"]=version["full_name"]
                        else:
                            if not version["full_name"] in entry["aliases"]:
                                entry["aliases"].append(version["full_name"])
                        if not entry["first_names"]:
                            entry["first_names"]=version["first_names"]
                        if not entry["last_names"]:
                            entry["last_name"]=version["last_names"]
                        if not entry["initials"]:
                            entry["initials"]=version["initials"]
                        if "corresponding" in version.keys():
                            if version["corresponding"] != "" and entry["corresponding"]=="":
                                entry["corresponding"]=version["corresponding"]
                        if "corresponding_email" in version.keys():
                            if version["corresponding_email"] != "" and entry["corresponding_email"]=="":
                                entry["corresponding_email"]=version["corresponding_email"]
                        if "corresponding_address" in version.keys():
                            if version["corresponding_address"] != "" and entry["corresponding_address"]=="":
                                entry["corresponding_address"]=version["corresponding_address"]
                        entry["external_ids"]=version["external_ids"] if "external_ids" in version.keys() else []
                #end of scopus update
                if scholar:
                    version={}
                    for author in scholar:
                        ratio=fuzz.partial_ratio(author["full_name"],scielo[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],scielo[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],scielo[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if version["external_ids"]:
                            entry["external_ids"].extend(version["external_ids"])
                        if not version["full_name"] in entry["full_name"]:
                            entry["aliases"].append(version["full_name"])

                entry["updated"]=updated      
                authors.append(entry)

        #End of scielo section

        elif scopus:
            author_count=len(scopus)
            print("Procesing ",author_count," authors")
            for i in range(author_count):
                entry={}
                entry["aliases"]=[]
                entry["external_ids"]=[]
                entry["corresponding"]=""
                entry["corresponding_address"]=""
                entry["corresponding_email"]=""

                entry["full_name"]=scopus[i]["full_name"] if "full_name" in scopus[i].keys() else ""
                entry["first_names"]=scopus[i]["first_names"] if "first_names" in scopus[i].keys() else ""
                entry["last_names"]=scopus[i]["last_names"] if "last_names" in scopus[i].keys() else ""
                entry["initials"]=scopus[i]["initials"] if "initials" in scopus[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in scopus[i].keys():
                    if scopus[i]["corresponding"] != "":
                        entry["corresponding"]=scopus[i]["corresponding"]
                if "corresponding_email" in scopus[i].keys():
                    if scopus[i]["corresponding_email"] != "":
                        entry["corresponding_email"]=scopus[i]["corresponding_email"]
                else:
                    entry["corresponding_email"]=""
                if "corresponding_address" in scopus[i].keys():
                    if scopus[i]["corresponding_address"] != "":
                        entry["corresponding_address"]=scopus[i]["corresponding_address"]

                if scholar:
                    version={}
                    for author in scholar:
                        ratio=fuzz.partial_ratio(author["full_name"],scopus[i]["full_name"])
                        if ratio>90:
                            version=author
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(author["full_name"],scopus[i]["full_name"])
                            if ratio>90:
                                version=author
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(author["full_name"],scopus[i]["full_name"])
                                if ratio>90:
                                    version=author
                                    break
                    if version:
                        if version["external_ids"]:
                            entry["external_ids"].extend(version["external_ids"])
                        if not version["full_name"] in entry["full_name"]:
                            entry["aliases"].append(version["full_name"])

                entry["updated"]=updated      
                authors.append(entry)
        
        return authors
        

    def find_grid(self,token,url='https://api.ror.org/organizations?affiliation='):
        print("SEARCHING FOR: ",token," IN ROR DB")
        query=urllib.parse.quote(token)
        url='{}{}'.format(url,query)
        result=requests.get(url)
        print(result)
        return result.json()

    def join_institutions(self,scholar=None,scopus=None,scielo=None,wos=None,lens=None):
        """
        Join institutions information from the given sources
        TODO: support of multiple affiliations
        * Search the institution by grid.id
        * if not grid available search by token provided within the names in grid (load them in ram)
        * If available add the wos/scielo institution name as alias
        * If none of the avobe worked add scopus institution (add where?)

        Parameters
        ----------
        scholar : list
            Institutions information from google scholar
        scopus : list
            Institutions information from scopus
        scielo : list
            Institutions information from scielo
        wos : list
            Institutions information from web of science
        lens : list
            Institutions information from lens
        
        Returns
        -------
        institutions : list
            Aggregated institutions information in CoLav standard
        """
        if self.verbose==5: print("JOINING INSTITUTIONS")
        institutions=[]
        updated=int(time())

        institutions_count=0
        institutions_found=0
        if lens:
            institutions_count=len(lens)
            print("Searching: ",institutions_count," institutions.")
            for i in range(institutions_count):
                entry={}
                aliases=[]
                entry["id"]=""
                entry["aliases"]=[]
                entry["author"]=""
                entry["countries"]=[]
                if len(lens[i])==0 or not lens[i]:
                    print("No institution to find")
                elif lens[i]["grid_id"]:
                    response=self.griddb["stage"].find_one({"id":lens[i]["grid_id"]})
                    entry["id"]=response["_id"]
                    entry["author"]=lens[i]["author"]
                    print("Found institution: ",lens[i]["name"])
                    institutions_found+=1
                    aliases.append(lens[i]["name"])
                    #maybe put here the aliases from other raw sources
                    entry["aliases"]=list(set(aliases))
                    self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                elif lens[i]["grid_id"]=="" and lens[i]["name"]!="":
                    result=self.find_grid(token=lens[i]["name"])
                    if result["number_of_results"]!=0:
                        if result["items"][0]["score"]>0.9:
                            try:
                                gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                db_institution=self.griddb["stage"].find_one({"id":gridid})
                                entry["id"]=db_institution["_id"]
                            except:
                                if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                            entry["author"]=lens[i]["author"]
                            version={}
                            if wos:
                                for institution in wos:
                                    #print("COMPARING ",institution["name"]," WITH ",lens[i]["name"])
                                    #print("partial ratio: ",fuzz.partial_ratio(institution["name"],lens[i]["name"]))
                                    #print("token set ratio: ",fuzz.token_set_ratio(institution["name"],lens[i]["name"]))
                                    #print("partial token set ratio: ",fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"]))
                                    ratio=fuzz.partial_ratio(institution["name"],lens[i]["name"])
                                    if ratio>90:
                                        version=institution
                                        break
                                    elif ratio>50:
                                        ratio=fuzz.token_set_ratio(institution["name"],lens[i]["name"])
                                        if ratio>90:
                                            version=institution
                                            break
                                        elif ratio>50:
                                            ratio=fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"])
                                            if ratio>90:
                                                version=institution
                                                break
                            elif scielo:
                                for institution in scielo:
                                    ratio=fuzz.partial_ratio(institution["name"],lens[i]["name"])
                                    if ratio>90:
                                        version=institution
                                        break
                                    elif ratio>50:
                                        ratio=fuzz.token_set_ratio(institution["name"],lens[i]["name"])
                                        if ratio>90:
                                            version=institution
                                            break
                                        elif ratio>50:
                                            ratio=fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"])
                                            if ratio>90:
                                                version=institution
                                                break
                            elif scopus:
                                for institution in scopus:
                                    #print("COMPARING ",institution["name"]," WITH ",lens[i]["name"])
                                    #print("partial ratio: ",fuzz.partial_ratio(institution["name"],lens[i]["name"]))
                                    #print("token set ratio: ",fuzz.token_set_ratio(institution["name"],lens[i]["name"]))
                                    #print("partial token set ratio: ",fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"]))
                                    ratio=fuzz.partial_ratio(institution["name"],lens[i]["name"])
                                    if ratio>90:
                                        version=institution
                                        break
                                    elif ratio>50:
                                        ratio=fuzz.token_set_ratio(institution["name"],lens[i]["name"])
                                        if ratio>90:
                                            version=institution
                                            break
                                        elif ratio>50:
                                            ratio=fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"])
                                            if ratio>90:
                                                version=institution
                                                break
                            
                            print(version)
                            try:
                                entry["countries"]=version["countries"]
                            except:
                                entry["countries"]=[]
                            if db_institution:
                               if "name" in db_institution.keys(): print("Found institution: ",db_institution["name"],", with token: ",lens[i]["name"])
                            else:
                                print("Could not find institution: ",lens[i]["name"])
                            institutions_found+=1
                            aliases.append(lens[i]["name"])
                            #maybe put here aliases from other raw sources
                            entry["aliases"]=list(set(aliases))
                            self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                        elif wos:
                            #check if some institution in wos is the same we are dealing with
                            wos_version={"name":""}
                            for institution in wos:
                                #print("COMPARING ",institution["name"]," WITH ",lens[i]["name"])
                                #print("partial ratio: ",fuzz.partial_ratio(institution["name"],lens[i]["name"]))
                                #print("token set ratio: ",fuzz.token_set_ratio(institution["name"],lens[i]["name"]))
                                #print("partial token set ratio: ",fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"]))
                                ratio=fuzz.partial_ratio(institution["name"],lens[i]["name"])
                                if ratio>90:
                                    wos_version=institution
                                    break
                                elif ratio>50:
                                    ratio=fuzz.token_set_ratio(institution["name"],lens[i]["name"])
                                    if ratio>90:
                                        wos_version=institution
                                        break
                                    elif ratio>50:
                                        ratio=fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"])
                                        if ratio>90:
                                            wos_version=institution
                                            break
                            try:
                                result=self.find_grid(token=wos_version["name"])
                            except:
                                result={}
                                result["number_of_results"]=1
                                result["items"]=[{"score":0}]
                            if result["number_of_results"]!=0:
                                if result["items"][0]["score"]>0.9:
                                    try:
                                        gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                        db_institution=self.griddb["stage"].find_one({"id":gridid})
                                        entry["id"]=db_institution["_id"]
                                    except:
                                        if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                                    entry["author"]=wos_version["author"]
                                    entry["countries"]=wos_version["countries"]
                                    if db_institution:print("Found institution: ",db_institution["name"],", with token: ",wos_version["name"])
                                    institutions_found+=1
                                    aliases.append(wos_version["name"])
                                    #maybe put here some aliases from other raw sources
                                    entry["aliases"]=list(set(aliases))
                                    self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                                elif scielo:
                                    #check if some institution in wos is the same we are dealing with
                                    scielo_version={}
                                    for institution in scielo:
                                        ratio=fuzz.partial_ratio(institution["name"],lens[i]["name"])
                                        if ratio>90:
                                            scielo_version=institution
                                            break
                                        elif ratio>50:
                                            ratio=fuzz.token_set_ratio(institution["name"],lens[i]["name"])
                                            if ratio>90:
                                                scielo_version=institution
                                                break
                                            elif ratio>50:
                                                ratio=fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"])
                                                if ratio>90:
                                                    scielo_version=institution
                                                    break
                                    try:
                                        result=self.find_grid(token=scielo_version["name"])
                                    except:
                                        result={}
                                        result["number_of_results"]=1
                                        result["items"]=[{"score":0}]
                                    if result["number_of_results"]!=0:
                                        if result["items"][0]["score"]>0.9:
                                            try:
                                                gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                                db_institution=self.griddb["stage"].find_one({"id":gridid})
                                                entry["id"]=db_institution["_id"]
                                            except:
                                                if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                                            entry["author"]=scielo_version["author"]
                                            entry["countries"]=scielo_version["countries"]
                                            if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scielo_version["name"])
                                            institutions_found+=1
                                            aliases.append(scielo_version["name"])
                                            entry["aliases"]=list(set(aliases))
                                            self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                                        elif scopus:
                                            scopus_version={}
                                            for institution in scopus:
                                                ratio=fuzz.partial_ratio(institution["name"],lens[i]["name"])
                                                if ratio>90:
                                                    scopus_version=institution
                                                    break
                                                elif ratio>50:
                                                    ratio=fuzz.token_set_ratio(institution["name"],lens[i]["name"])
                                                    if ratio>90:
                                                        scopus_version=institution
                                                        break
                                                    elif ratio>50:
                                                        ratio=fuzz.partial_token_set_ratio(institution["name"],lens[i]["name"])
                                                        if ratio>90:
                                                            scopus_version=institution
                                                            break
                                            try:
                                                result=self.find_grid(token=scopus_version["name"])
                                            except:
                                                result={}
                                                result["number_of_results"]=1
                                                result["items"]=[{"score":0}]
                                            if result["number_of_results"]!=0:
                                                if result["items"][0]["score"]>0.9:
                                                    try:
                                                        gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                                        db_institution=self.griddb["stage"].find_one({"id":gridid})
                                                        entry["id"]=db_institution["_id"]
                                                    except:
                                                        if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                                                    entry["author"]=scopus_version["author"]
                                                    if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scopus_version["name"])
                                                    institutions_found+=1
                                                    aliases.append(scopus_version["name"])
                                                    entry["aliases"]=list(set(aliases))
                                                    self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}}) 
                else: #If there is an author but no institution gridid or name
                    
                    print("Institution not found")
                institutions.append(entry)            
                if institutions_count==institutions_found:
                    print("FOUND ALL INSTITUTIONS")
            print(len(institutions))
            return institutions
        #END OF LENS PORTION
        elif wos: #if not lens at all
            institutions_count=len(wos)
            print("Searching: ",institutions_count," institutions.")
            for i in range(institutions_count):
                entry={}
                aliases=[]
                entry["id"]=""
                entry["aliases"]=[]
                entry["author"]=""
                entry["countries"]=wos[i]["countries"]
                if not wos[i]["name"]: continue
                result=self.find_grid(token=wos[i]["name"])
                if result["number_of_results"]!=0:
                    if result["items"][0]["score"]>0.9:
                        try:
                            gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                            db_institution=self.griddb["stage"].find_one({"id":gridid})
                            entry["id"]=db_institution["_id"]
                        except:
                            if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                        entry["author"]=wos[i]["author"]
                        if db_institution:print("Found institution: ",db_institution["name"],", with token: ",wos[i]["name"])
                        institutions_found+=1
                        aliases.append(wos[i]["name"])
                        entry["aliases"]=list(set(aliases))
                        self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                    elif scielo:
                        scielo_version={}
                        for institution in scielo:
                            ratio=fuzz.partial_ratio(institution["name"],wos[i]["name"])
                            if ratio>90:
                                scielo_version=institution
                                break
                            elif ratio>50:
                                ratio=fuzz.token_set_ratio(institution["name"],wos[i]["name"])
                                if ratio>90:
                                    scielo_version=institution
                                    break
                                elif ratio>50:
                                    ratio=fuzz.partial_token_set_ratio(institution["name"],wos[i]["name"])
                                    if ratio>90:
                                        scielo_version=institution
                                        break
                        try:
                            result=self.find_grid(token=scielo_version["name"])
                        except:
                            result={}
                            result["number_of_results"]=1
                            result["items"]=[{"score":0}]
                        if result["number_of_results"]!=0:
                            if result["items"][0]["score"]>0.9:
                                try:
                                    gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                    db_institution=self.griddb["stage"].find_one({"id":gridid})
                                    entry["id"]=db_institution["_id"]
                                except:
                                    if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                                entry["author"]=scielo_version["author"]
                                entry["countries"]=scielo_version["countries"]
                                if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scielo_version["name"])
                                institutions_found+=1
                                aliases.append(scielo_version["name"])
                                entry["aliases"]=list(set(aliases))
                                self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                            elif scopus:
                                scopus_version={}
                                for institution in scopus:
                                    ratio=fuzz.partial_ratio(institution["name"],wos[i]["name"])
                                    if ratio>90:
                                        scopus_version=institution
                                        break
                                    elif ratio>50:
                                        ratio=fuzz.token_set_ratio(institution["name"],wos[i]["name"])
                                        if ratio>90:
                                            scopus_version=institution
                                            break
                                        elif ratio>50:
                                            ratio=fuzz.partial_token_set_ratio(institution["name"],wos[i]["name"])
                                            if ratio>90:
                                                scopus_version=institution
                                                break
                                try:
                                    result=self.find_grid(token=scopus_version["name"])
                                except:
                                    result={}
                                    result["number_of_results"]=1
                                    result["items"]=[{"score":0}]
                                if result["number_of_results"]!=0:
                                    if result["items"][0]["score"]>0.9:
                                        try:
                                            gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                            db_institution=self.griddb["stage"].find_one({"id":gridid})
                                            entry["id"]=db_institution["_id"]
                                        except:
                                            if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                                        entry["author"]=scopus_version["author"]
                                        if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scopus_version["name"])
                                        institutions_found+=1
                                        aliases.append(scopus_version["name"])
                                        entry["aliases"]=list(set(aliases))
                                        self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                else:
                    print("Institution not found")
                institutions.append(entry)            
                if institutions_count==institutions_found:
                    print("FOUND ALL INSTITUTIONS")
            print(len(institutions))
            return institutions
            #END OF WOS PORTION

        elif scielo:
            institutions_count=len(scielo)
            print("Searching: ",institutions_count," institutions.")
            for i in range(institutions_count):
                entry={}
                aliases=[]
                entry["id"]=""
                entry["aliases"]=[]
                entry["author"]=""
                entry["countries"]=scielo[i]["countries"]
                if not scielo[i]["name"]: continue
                result=self.find_grid(token=scielo[i]["name"])
                if result["number_of_results"]!=0:
                    if result["items"][0]["score"]>0.9:
                        try:
                            gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                            db_institution=self.griddb["stage"].find_one({"id":gridid})
                            entry["id"]=db_institution["_id"]
                        except:
                            if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                        if scielo[i]["author"]:
                            entry["author"]=scielo[i]["author"]
                        else:
                            if scopus:
                                for institution in scopus:
                                    if fuzz.partial_ratio(institution["name"],scielo[i]["name"])>90:
                                        entry["author"]=institution["author"]
                        if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scielo[i]["name"])
                        institutions_found+=1
                        aliases.append(scielo[i]["name"])
                        entry["aliases"]=list(set(aliases))
                        self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                    elif scopus:
                        scopus_version={}
                        for institution in scopus:
                            ratio=fuzz.partial_ratio(institution["name"],scielo[i]["name"])
                            if ratio>90:
                                scopus_version=institution
                                break
                            elif ratio>50:
                                ratio=fuzz.token_set_ratio(institution["name"],scielo[i]["name"])
                                if ratio>90:
                                    scopus_version=institution
                                    break
                                elif ratio>50:
                                    ratio=fuzz.partial_token_set_ratio(institution["name"],scielo[i]["name"])
                                    if ratio>90:
                                        scopus_version=institution
                                        break
                        try:
                            result=self.find_grid(token=scopus_version["name"])
                        except:
                            result={}
                            result["number_of_results"]=1
                            result["items"]=[{"score":0}]
                        if result["number_of_results"]!=0:
                            if result["items"][0]["score"]>0.9:
                                try:
                                    gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                                    db_institution=self.griddb["stage"].find_one({"id":gridid})
                                    entry["id"]=db_institution["_id"]
                                except:
                                    if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                                entry["author"]=scopus_version["author"]
                                if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scopus_version["name"])
                                institutions_found+=1
                                aliases.append(scopus_version["name"])
                                entry["aliases"]=list(set(aliases))
                                self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                else:
                    print("Institution not found")
                institutions.append(entry)            
                if institutions_count==institutions_found:
                    print("FOUND ALL INSTITUTIONS")
            return institutions
            #END OF SCIELO PORTION

        elif scopus:
            institutions_count=len(scopus)
            print("Searching: ",institutions_count," institutions.")
            for i in range(institutions_count):
                entry={}
                aliases=[]
                entry["id"]=""
                entry["aliases"]=[]
                entry["author"]=""
                if not scopus[i]["name"]: continue
                result=self.find_grid(token=scopus[i]["name"])
                if result["number_of_results"]!=0:
                    if result["items"][0]["score"]>0.9:
                        try:
                            gridid=result["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                            db_institution=self.griddb["stage"].find_one({"id":gridid})
                            entry["id"]=db_institution["_id"]
                        except:
                            if self.verbose>2:print("Could not find id ",gridid,"in GRID db")
                        entry["author"]=scopus[i]["author"]
                        if db_institution:print("Found institution: ",db_institution["name"],", with token: ",scopus[i]["name"])
                        institutions_found+=1
                        aliases.append(scopus[i]["name"])
                        entry["aliases"]=list(set(aliases))
                        self.griddb["stage"].update_one({"_id":entry["id"]},{"$push":{"aliases":entry["aliases"]}})
                else:
                    print("Institution not found")
                institutions.append(entry)            
                if institutions_count==institutions_found:
                    print("FOUND ALL INSTITUTIONS")
            print(len(institutions))
            return institutions
            #END OF SCOPUS PORTION
    
    def join_authors_and_institutions(self,raw_authors,raw_institutions):
        authors=self.join_authors(lens=raw_authors[0],
                                   wos=raw_authors[1],
                                   scielo=raw_authors[2],
                                   scopus=raw_authors[3],
                                   scholar=raw_authors[4])
        institutions=self.join_institutions(lens=raw_institutions[0],
                                            wos=raw_institutions[1],
                                            scielo=raw_institutions[2],
                                            scopus=raw_institutions[3],
                                            scholar=raw_institutions[4])
        #institution_authors=[inst["author"] for inst in institutions]

        if self.verbose>4:
            print("\n--------------------\n")
            print(len(authors))
            print(authors)
            print("\n")
            print(len(institutions))
            print(institutions)
            print("\n--------------------\n")

        for author in authors:
            author["affiliations"]=[]
            for institution in institutions:
                if institution["author"]=="":continue
                if fuzz.token_set_ratio(author["full_name"],institution["author"])>90:
                    inst=institution.copy()
                    del(inst["aliases"])
                    del(inst["author"])
                    author["affiliations"].append(inst)
        for institution in institutions: #searching empty institutions and append a false author
            if "author" in institution.keys():
                if institution["author"]=="":
                    entry={
                        "full_name":"Regina Falange",
                        "first_names":"Regina",
                        "last_names":"Falange",
                        "initials":"R",
                        "external_ids":[],
                        "aliases":[],
                        "corresponding":False,
                        "corresponding_address":"",
                        "corresponding_email":"",
                        "updated":666,
                        "affiliations":[institution]
                    }
                    authors.append(entry)        




        return authors


    def join_source(self,scholar=None,scopus=None,scielo=None,wos=None,lens=None):
        """
        Join the source information from the given sources

        Parameters
        ----------
        scholar : dict
            Source information from google scholar
        scopus : dict
            Source information from scopus
        scielo : dict
            Source information from scielo
        wos : dicst
            Source information from web of science
        lens : dict
            Source information from lens
        
        Returns
        -------
        source : dict
            Aggregated source information in CoLav standard
        """
        if self.verbose==5: print("JOINING SOURCES")
        source={}
        source["updated"]=int(time())
        source["dbs"]=[]

        if scopus:
            source["dbs"].append("scopus")
        if scielo:
            source["dbs"].append("scielo")
        if wos:
            source["dbs"].append("wos")
        if scholar:
            source["dbs"].append("scholar")
        if lens:
            source["dbs"].append("lens")

        #title
        if scopus:
            source["title"]=scopus["title"] if scopus["title"] else ""
        else:
            source["title"]=""
        if wos:
            if wos["title"]:
                source["title"]=wos["title"]
        if scielo:
            if scielo["title"]:
                source["title"]=scielo["title"]
        if lens:
            if lens["title"]:
                source["title"]=lens["title"]
        source["title_idx"]=source["title"].lower()

        #Publisher
        if scopus:
            source["publisher"]=scopus["publisher"] if scopus["publisher"] else ""
        else:
            source["publisher"]=""
        if scielo:
            if scielo["publisher"]:
                source["publisher"]=scielo["publisher"]
        if wos:
            if wos["publisher"]:
                source["publisher"]=wos["publisher"]
        if lens:
            if lens["publisher"]:
                source["publisher"]=lens["publisher"]

        source["publisher_idx"]=source["publisher"].lower()
        
        #Country
        if lens:
            source["country"]=lens["country"] if lens["country"] else ""
        else:
            source["country"]=""
        
        #Type
        if scielo:
            source["type"]=scielo["type"] if scielo["type"] else ""
        else:
            source["type"]=""
        if wos:
            if wos["type"]:
                source["type"]=wos["type"]

        #Abbreviations
        abbreviations=[]
        abbreviations_values=[]
        if wos:
            for abb in wos["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
        if scopus:
            for abb in scopus["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
        if scielo:
            for abb in scielo["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
        if lens:
            for abb in lens["abbreviations"]:
                if not abb["value"] in abbreviations_values:
                    abbreviations.append(abb)
                    abbreviations_values.append(abb["value"])
        source["abbreviations"]=abbreviations


        #Serials
        serials=[]
        serials_values=[]
        pissn=""
        eissn=""
        if lens:
            for serial in lens["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
                    if serial["type"]=="eissn": eissn=serial["value"]
                    if serial["type"]=="pissn": pissn=serial["value"]
        if wos:
            for serial in wos["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
        if scielo:
            for serial in scielo["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
        if scopus:
            for serial in scopus["serials"]:
                if not serial["value"] in serials_values:
                    serials.append(serial)
                    serials_values.append(serial["value"])
        source["serials"]=serials

        doaj=None
        source["submission_charges_usd"]=""
        source["submission_charges"]=""
        source["submission_currency"]=""
        source["apc_charges"]=""
        source["apc_currency"]=""
        if eissn:
            #print(eissn)
            doaj=self.doajdb["stage"].find_one({"bibjson.identifier.id":eissn[:4]+"-"+eissn[4:]})
        elif pissn:
            #print(pissn)
            doaj=self.doajdb["stage"].find_one({"bibjson.identifier.id":pissn[:4]+"-"+pissn[4:]})
        if doaj:
            a,b,c,doaj=self.doaj_parser.parse_one(doaj)
        if doaj:
            source["submission_charges"]=doaj["submission_charges"]
            source["submission_currency"]=doaj["submission_currency"]
            if source["submission_currency"]:
                try:
                    source["submission_charges_usd"]=self.currecy.convert(doaj["submission_charges"],doaj["submission_currency"],"USD")
                except:
                    source["submission_charges_usd"]=""
            else:
                source["submission_charges_usd"]=""
            source["apc_charges"]=doaj["apc_charges"]
            source["apc_currency"]=doaj["apc_currency"]
            if source["apc_currency"]:
                try:
                    source["apc_charges_usd"]=self.currecy.convert(doaj["apc_charges"],doaj["apc_currency"],"USD")
                except:
                    source["apc_charges_usd"]=""
            else:
                source["apc_charges_usd"]=""

        return source



    def update_one(self,doi,insert=True):
        """
        Retrieve all updatable data from all the raw dbs
        Parse each register
        Find the same document through DOI
        Build the CoLav formatted register according to a hierarchy
        Save the register in the CoLav db, if there is no register already there with the same DOI
        """
        #check if doi alredy in the db
        documentdb=None
        documentdb=self.colavdb["documents"].find_one({"external_ids.id":doi})
        if self.verbose>2: print("Parsing DOI: ",doi)
        if documentdb:
            if self.verbose>3:
                print("Document already in db. Skipping")
                return 0
            else: pass
        
        wosregister=self.wosdb["stage"].find_one({"DI":doi})
        if wosregister:
            wosdoc,wosau,wosinst,wossource=self.wos_parser.parse_one(wosregister)
        else:
            wosdoc,wosau,wosinst,wossource=[None,None,None,None]
 
        lensregister=self.lensdb["stage"].find_one({"external_ids.value":doi})
        if lensregister:
            lensdoc,lensau,lensinst,lenssource=self.lens_parser.parse_one(lensregister)
        else:
            lensdoc,lensau,lensinst,lenssource=[None,None,None,None]

        scieloregister=self.scielodb["stage"].find_one({"DI":doi})
        if scieloregister:
            scielodoc,scieloau,scieloinst,scielosource=self.scielo_parser.parse_one(scieloregister)
        else:
            scielodoc,scieloau,scieloinst,scielosource=[None,None,None,None]

        scopusregister=self.scopusdb["stage"].find_one({"DOI":doi})
        if scopusregister:
            scopusdoc,scopusau,scopusinst,scopussource=self.scopus_parser.parse_one(scopusregister)
        else:
            scopusdoc,scopusau,scopusinst,scopussource=[None,None,None,None]

        scholarregister=self.scholardb["stage"].find_one({"doi":doi})
        if scholarregister:
            scholardoc,scholarau,scholarinst,scholarsource=self.scholar_parser.parse_one(scholarregister)
        else:
            scholardoc,scholarau,scholarinst,scholarsource=[None,None,None,None]

        oadoiregister=self.oadoidb["stage"].find_one({"doi":doi})
        if oadoiregister:
            oadoidoc,oadoiau,oadoiinst,oadoisource=self.oadoi_parser.parse_one(oadoiregister)
        else:
            oadoidoc,oadoiau,oadoiinst,oadoisource=[None,None,None,None]


        if self.verbose==5:
            print("*********\n** wos **\n*********")
            print(wosdoc)
            print(wosau)
            print(wosinst)
            print(wossource)
            print("\n")
            print("**********\n** lens **\n**********")
            print(lensdoc)
            print(lensau)
            print(lensinst)
            print(lenssource)
            print("\n")
            print("**********\n* scielo *\n**********")
            print(scielodoc)
            print(scieloau)
            print(scieloinst)
            print(scielosource)
            print("\n")
            print("**********\n* scopus *\n**********")
            print(scopusdoc)
            print(scopusau)
            print(scopusinst)
            print(scopussource)
            print("\n")
            print("**********\n* scholar *\n**********")
            print(scholardoc)
            print(scholarau)
            print("\n")
            print("**********\n* oadoi *\n**********")
            print(oadoidoc)

            print("\n--------------------\n")

        document=self.join_document(scholar=scholardoc,lens=lensdoc,scopus=scopusdoc,scielo=scielodoc,wos=wosdoc,oadoi=oadoidoc)
        
        #authors=self.join_authors(scholar=scholarau,lens=lensau,scopus=scopusau,scielo=scieloau,wos=wosau)
        #institutions=self.join_institutions(lens=lensinst,scopus=scopusinst,scielo=scieloinst,wos=wosinst)
        raw_authors=(lensau,wosau,scieloau,scopusau,scholarau)
        raw_institutions=(lensinst,wosinst,scieloinst,scopusinst,scholarinst)
        authors=self.join_authors_and_institutions(raw_authors,raw_institutions)

        source=self.join_source(lens=lenssource,scopus=scopussource,scielo=scielosource,wos=wossource)

        if self.verbose>3:
            print(document)
            print("\n")
            print(authors)
            print("\n")
            print(source)

            print("\n--------------------\n")

            
        #Check if author already in the db, update the document register
        author_ids=[]
        for author in authors:
            authordb=None
            aliases=[]
            for ext in author["external_ids"]:
                authordb=self.colavdb["authors"].find_one({"external_ids.value":ext["value"]})
                if authordb:
                    print("Author found with: ",ext)
                    #print(authordb)
                    print(":)")
                    break
            if authordb:
                author_ids.append(authordb["_id"])
                dbid=authordb["_id"]
                #update alias of the author
                
                aliases=authordb["aliases"]
                aliases.extend(author["aliases"])
                ali=list(set(aliases))
                mod={"$set":{"aliases":ali,"updated":author["updated"]}}
                if insert: self.colavdb["authors"].update_one({"_id":authordb["_id"]},mod)
            else:
                entry=author.copy()
                print("Author not found. Inserting")
                #print(entry)
                del(entry["corresponding"])
                del(entry["corresponding_email"])
                del(entry["corresponding_address"])
                del(entry["affiliations"])
                #print(entry)
                if insert:
                    result=self.colavdb["authors"].insert_one(entry)
                    dbid=result.inserted_id
                    author_ids.append(dbid)
                else:
                    dbid=""
            author["id"]=dbid
            del(author["aliases"])
            del(author["external_ids"])
            del(author["full_name"])
            del(author["first_names"])
            del(author["last_names"])
            del(author["initials"])

        #check if source already in the db, update the register
        sourcedb=None
        eissn=""
        pissn=""
        for serial in source["serials"]:
            eissn=serial["value"] if serial["type"]=="eissn" else ""
            pissn=serial["value"] if serial["type"]=="pissn" else ""
        if eissn:
            sourcedb=self.colavdb["sources"].find_one({"serials.value":eissn})
        elif pissn:
            sourcedb=self.colavdb["sources"].find_one({"serials.value":pissn})
        if not sourcedb:
            if insert:
                result=self.colavdb["sources"].insert_one(source)
                source_id=result.inserted_id
            else:
                source_id=""
        else:
            source_id=sourcedb["_id"]
        
        #Assemble the document with source, author and affiliation ids
        #update affiliation information
        #update author information
        document["authors"]=authors
        #update source information
        document["source"]=source_id
        if self.verbose>3:print(document)
        #insert the document
        if insert: result=self.colavdb["documents"].insert_one(document)
        else: result=""
        

    def update(self,num_jobs=8,lens=1,wos=0,scielo=0,scopus=0):
        #Get the list of al unique dois in the raw dbs
        #doi_list=['10.15446/ing.investig.v35n3.49498','10.15446/historelo.v7n14.47784',
        #'10.15446/cuad.econ.v35n68.52801','10.17230/co-herencia.16.30.6',
        #'10.22319/rmcp.v10i2.4652','10.18046/j.estger.2018.147.2643',
        #'10.1590/1518-8345.0000.2717','10.2225/vol16-issue2-fulltext-4',
        #'10.15446/ideasyvalores.v64n158.40109']
        
        lens_list=[]
        for reg in self.lensdb["stage"].find():
            if "external_ids" in reg.keys():
                if reg["external_ids"]:
                    for ext in reg["external_ids"]:
                        if ext["type"]=="doi":
                            lens_list.append(ext["value"])
        if lens:
            Parallel(n_jobs=num_jobs,backend="threading",verbose=10)(delayed(self.update_one)(doi) for doi in lens_list)

        
        wos_list=[]
        for reg in self.wosdb["stage"].find():
            if "DI" in reg.keys():
                if reg["DI"]:
                    wos_list.append(reg["DI"])
        wos_not_lens_list=set(wos_list)-set(wos_list).intersection(lens_list)
        if wos:
            Parallel(n_jobs=num_jobs,backend="threading",verbose=10)(delayed(self.update_one)(doi) for doi in wos_not_lens_list)

        scielo_list=[]
        for reg in self.scielodb["stage"].find():
            if "DI" in reg.keys():
                if reg["DI"]:
                    scielo_list.append(reg["DI"])
        lens_list.extend(wos_list)
        uniq=set(lens_list)
        scielo_not_wos_not_lens_list=set(scielo_list)-set(scielo_list).intersection(uniq)
        if scielo:
            Parallel(n_jobs=num_jobs,backend="threading",verbose=10)(delayed(self.update_one)(doi) for doi in scielo_not_wos_not_lens_list)

        scopus_list=[]
        for reg in self.scopusdb["stage"].find():
            if "DOI" in reg.keys():
                if reg["DOI"]:
                    scopus_list.append(reg["DOI"])
        lens_list.extend(scielo_list)
        uniq=set(lens_list)
        scopus_not_scielo_not_wos_not_lens_list=set(scopus_list)-set(scopus_list).intersection(uniq)
        if scopus:
            Parallel(n_jobs=num_jobs,backend="threading",verbose=10)(delayed(self.update_one)(doi) for doi in scopus_not_scielo_not_wos_not_lens_list)
