from pymongo import MongoClient
import json
from time import time
from langdetect import detect
from currency_converter import CurrencyConverter
#import Levenshtein

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

        self.colav=self.client["colav"]

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
                lang=detect(title)
                if not lang in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang)
                    titles_idx.append(title.lower())
        if scielo:
            if scielo["title"]:
                title=scielo["title"]
                lang=detect(title)
                if not lang in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang)
                    titles_idx.append(title.lower())
        if wos:
            if wos["title"]:
                title=wos["title"]
                lang=detect(title)
                if not lang in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang)
                    titles_idx.append(title.lower())
        if scopus:
            if scopus["title"]:
                title=scopus["title"]
                lang=detect(title)
                if not lang in titles_lang:
                    titles.append(title)
                    titles_lang.append(lang)
                    titles_idx.append(title.lower())
        
        for idx,title in enumerate(titles): 
            document["titles"].append({"title":title,"lang":titles_lang[idx],"title_idx":titles_idx[idx]})

        #abstract
        if scopus:
            document["abstract"]=scopus["abstract"] if scopus["abstract"] else ""
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
        if scopus:
            document["year_published"]=scopus["year_published"] if scopus["year_published"] else ""
        if scielo:
            if scielo["year_published"]:
                document["year_published"]=scielo["year_published"]
        if wos:
            if wos["year_published"]:
                document["year_published"]=wos["year_published"]
        if lens:
            if lens["year_published"]:
                document["year_published"]=lens["year_published"]

        #author count
        if scopus:
            document["author_count"]=scopus["author_count"] if scopus["author_count"] else ""
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
        authors=[]
        names=[]
        author_count=0
        if lens:
            if author_count!=0 and len(lens)!= author_count:
                print("Number of authors does not match")
                return(authors)
            author_count=len(lens)
        if wos:
            if author_count!=0 and len(wos)!= author_count:
                print("Number of authors does not match")
                return(authors)
            author_count=len(wos)
        if scielo:
            if author_count!=0 and len(scielo)!= author_count:
                print("Number of authors does not match")
                return(authors)
            author_count=len(scielo)
        if scopus:
            if author_count!=0 and len(scopus)!= author_count:
                print("Number of authors does not match")
                return(authors)
            author_count=len(scopus)

        updated=int(time())
        
        for i in range(author_count):
            entry={}
            entry["aliases"]=[]
            if scopus:
                entry["full_name"]=scopus[i]["full_name"] if "full_name" in scopus[i].keys() else ""
                entry["first_names"]=scopus[i]["first_names"] if "first_names" in scopus[i].keys() else ""
                entry["last_names"]=scopus[i]["last_names"] if "last_names" in scopus[i].keys() else ""
                entry["initials"]=scopus[i]["initials"] if "initials" in scopus[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                entry["external_ids"]=scopus[i]["external_ids"] if "external_ids" in scopus[i].keys() else ""
                if "corresponding" in scopus[i].keys():
                    if scopus[i]["corresponding"] != "":
                        entry["correspoding"]=scopus[i]["corresponding"]
                    else:
                        entry["correspoding"]=""
                if "corresponding_email" in scopus[i].keys():
                    if scopus[i]["corresponding_email"] != "":
                        entry["correspoding_email"]=scopus[i]["corresponding_email"]
                    else:
                        entry["correspoding_email"]=""
                if "corresponding_address" in scopus[i].keys():
                    if scopus[i]["corresponding_address"] != "":
                        entry["correspoding_address"]=scopus[i]["corresponding_address"]
                    else:
                        entry["correspoding_address"]=""
            if scielo:
                entry["full_name"]=scielo[i]["full_name"] if "full_name" in scielo[i].keys() else ""
                entry["first_names"]=scielo[i]["first_names"] if "first_names" in scielo[i].keys() else ""
                entry["last_names"]=scielo[i]["last_names"] if "last_names" in scielo[i].keys() else ""
                entry["initials"]=scielo[i]["initials"] if "initials" in scielo[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in scielo[i].keys():
                    if scielo[i]["corresponding"] != "":
                        entry["correspoding"]=scielo[i]["corresponding"]
                    else:
                        entry["correspoding"]=""
                if "corresponding_email" in scielo[i].keys():
                    if scielo[i]["corresponding_email"] != "":
                        entry["correspoding_email"]=scielo[i]["corresponding_email"]
                    else:
                        entry["correspoding_email"]=""
                if "corresponding_address" in scielo[i].keys():
                    if scielo[i]["corresponding_address"] != "":
                        entry["correspoding_address"]=scielo[i]["corresponding_address"]
                    else:
                        entry["correspoding_address"]=""
            if wos:
                entry["full_name"]=wos[i]["full_name"] if "full_name" in wos[i].keys() else ""
                entry["first_names"]=wos[i]["first_names"] if "first_names" in wos[i].keys() else ""
                entry["last_names"]=wos[i]["last_names"] if "last_names" in wos[i].keys() else ""
                entry["initials"]=wos[i]["initials"] if "initials" in wos[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
                if "corresponding" in wos[i].keys():
                    if wos[i]["corresponding"] != "":
                        entry["correspoding"]=wos[i]["corresponding"]
                    else:
                        entry["correspoding"]=""
                if "corresponding_email" in wos[i].keys():
                    if wos[i]["corresponding_email"] != "":
                        entry["correspoding_email"]=wos[i]["corresponding_email"]
                    else:
                        entry["correspoding_email"]=""
                if "corresponding_address" in wos[i].keys():
                    if wos[i]["corresponding_address"] != "":
                        entry["correspoding_address"]=wos[i]["corresponding_address"]
                    else:
                        entry["correspoding_address"]=""
            if lens:
                entry["full_name"]=lens[i]["full_name"] if "full_name" in lens[i].keys() else ""
                entry["first_names"]=lens[i]["first_names"] if "first_names" in lens[i].keys() else ""
                entry["last_names"]=lens[i]["last_names"] if "last_names" in lens[i].keys() else ""
                entry["initials"]=lens[i]["initials"] if "initials" in lens[i].keys() else ""
                if not entry["full_name"] in entry["aliases"]:
                    entry["aliases"].append(entry["full_name"])
            
            
            entry["updated"]=updated        
            authors.append(entry)
        return authors

    def join_institutions(self,scholar=None,scopus=None,scielo=None,wos=None,lens=None):
        """
        Join institutions information from the given sources
        TODO: support of multiple affiliations

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
        institutions=[]
        updated=int(time())

        institutions_count=0
        if lens:
            if institutions_count!=0 and len(lens)!= institutions_count:
                print("Number of institutions does not match")
                return(institutions)
            institutions_count=len(lens)
        if wos:
            if institutions_count!=0 and len(wos)!= institutions_count:
                print("Number of institutions does not match")
                return(institutions)
            institutions_count=len(wos)
        if scielo:
            if institutions_count!=0 and len(scielo)!= institutions_count:
                print("Number of institutions does not match")
                return(institutions)
            institutions_count=len(scielo)
        if scopus:
            if institutions_count!=0 and len(scopus)!= institutions_count:
                print("Number of institutions does not match")
                return(institutions)
            institutions_count=len(scopus)

        for i in range(institutions_count):
            entry={}
            entry["aliases"]=[]
            if wos:
                entry["name"]=wos[i]["name"] if "name" in wos[i].keys() else ""
                entry["country"]=wos[i]["country"] if "country" in wos[i].keys() else ""
                if not entry["name"] in entry["aliases"] and entry["name"]!="":
                    entry["aliases"].append(entry["name"])
            if scielo:
                entry["name"]=scielo[i]["name"] if "name" in scielo[i].keys() else ""
                entry["country"]=scielo[i]["country"] if "country" in scielo[i].keys() else ""
                if not entry["name"] in entry["aliases"] and entry["name"]!="":
                    entry["aliases"].append(entry["name"])
            if lens:
                entry["name"]=lens[i][0]["name"] if "name" in lens[i][0].keys() else ""
                entry["grid_id"]=lens[i][0]["grid_id"] if "grid_id" in lens[i][0].keys() else ""
                if not entry["name"] in entry["aliases"] and entry["name"]!="":
                    entry["aliases"].append(entry["name"])
            if scopus:
                entry["name"]=scopus[i]["name"] if "name" in scopus[i].keys() else ""
                if not entry["name"] in entry["aliases"] and entry["name"]!="":
                    entry["aliases"].append(entry["name"])
            
            entry["updated"]=updated
            institutions.append(entry)

        return institutions

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
        sourcce : dict
            Aggregated source information in CoLav standard
        """
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

        if eissn:
            doaj=self.doajdb["stage"].find_one({"bibjson.identifier.id":eissn[:4]+"-"+eissn[4:]})
        elif pissn:
            doaj=self.doajdb["stage"].find_one({"bibjson.identifier.id":pissn[:4]+"-"+pissn[4:]})
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



    def update(self,last_updated=None,test_idx=1):
        """
        Retrieve all updatable data from all the raw dbs
        Parse each register
        Find the same document through DOI
        Build the CoLav formatted register according to a hierarchy
        Save the register in the CoLav db, if there is no register already there with the same DOI
        """

        #Get the list of al unique dois in the raw dbs
        db_list=[self.lensdb,self.wosdb,self.scielodb,self.scopusdb]
        doi_list=['10.15446/ing.investig.v35n3.49498','10.15446/historelo.v7n14.47784',
        '10.15446/cuad.econ.v35n68.52801','10.17230/co-herencia.16.30.6',
        '10.22319/rmcp.v10i2.4652','10.18046/j.estger.2018.147.2643',
        '10.1590/1518-8345.0000.2717','10.2225/vol16-issue2-fulltext-4',
        '10.15446/ideasyvalores.v64n158.40109']

        """for reg in self.lensdb["stage"].find():
            if "external_ids" in reg.keys():
                if reg["external_ids"]:
                    for ext in reg["external_ids"]:
                        if ext["type"]=="doi":
                            doi_list.append(ext["value"])"""
        if self.verbose==5:
            print(len(doi_list))
        #Find those unique dois wherever they are in the raw dbs
        #Call the corresponding parser
        wosregister=self.wosdb["stage"].find_one({"DI":doi_list[test_idx]})
        if wosregister:
            wosdoc,wosau,wosinst,wossource=self.wos_parser.parse_one(wosregister)
        else:
            wosdoc,wosau,wosinst,wossource=[None,None,None,None]
 
        lensregister=self.lensdb["stage"].find_one({"external_ids.value":doi_list[test_idx]})
        if lensregister:
            lensdoc,lensau,lensinst,lenssource=self.lens_parser.parse_one(lensregister)
        else:
            lensdoc,lensau,lensinst,lenssource=[None,None,None,None]

        scieloregister=self.scielodb["stage"].find_one({"DI":doi_list[test_idx]})
        if scieloregister:
            scielodoc,scieloau,scieloinst,scielosource=self.scielo_parser.parse_one(scieloregister)
        else:
            scielodoc,scieloau,scieloinst,scielosource=[None,None,None,None]

        scopusregister=self.scopusdb["stage"].find_one({"DOI":doi_list[test_idx]})
        if scopusregister:
            scopusdoc,scopusau,scopusinst,scopussource=self.scopus_parser.parse_one(scopusregister)
        else:
            scopusdoc,scopusau,scopusinst,scopussource=[None,None,None,None]

        scholarregister=self.scholardb["stage"].find_one({"doi":doi_list[test_idx]})
        if scholarregister:
            scholardoc,scholarau,scholarinst,scholarsource=self.scholar_parser.parse_one(scholarregister)
        else:
            scholardoc,scholarau,scholarinst,scholarsource=[None,None,None,None]

        oadoiregister=self.oadoidb["stage"].find_one({"doi":doi_list[test_idx]})
        if oadoiregister:
            oadoidoc,oadoiau,oadoiinst,oadoisource=self.oadoi_parser.parse_one(oadoiregister)
        else:
            oadoidoc,oadoiau,oadoiinst,oadoisource=[None,None,None,None]


        if self.verbose==5:
            print(wosdoc)
            print(wosau)
            print(wosinst)
            print(wossource)
            print("\n")
            print(lensdoc)
            print(lensau)
            print(lensinst)
            print(lenssource)
            print("\n")
            print(scielodoc)
            print(scieloau)
            print(scieloinst)
            print(scielosource)
            print("\n")
            print(scopusdoc)
            print(scopusau)
            print(scopusinst)
            print(scopussource)
            print("\n")
            print(scholardoc)
            print("\n")
            print(oadoidoc)

            print("\n--------------------\n")

        document=self.join_document(scholar=scholardoc,lens=lensdoc,scopus=scopusdoc,scielo=scielodoc,wos=wosdoc,oadoi=oadoidoc)
        authors=self.join_authors(lens=lensau,scopus=scopusau,scielo=scieloau,wos=wosau)
        institutions=self.join_institutions(lens=lensinst,scopus=scopusinst,scielo=scieloinst,wos=wosinst)
        source=self.join_source(lens=lenssource,scopus=scopussource,scielo=scielosource,wos=wossource)

        if self.verbose>3:
            print(document)
            print("\n")
            print(authors)
            print("\n")
            print(institutions)
            print("\n")
            print(source)

        #Build the right register
        #Insert it in the final db
        #Check if institution already in db, update the doc
        #Check if author already in the db, update the doc
        #check if source already in the db, update the doc
        #check if doc alredy in the db
        

        