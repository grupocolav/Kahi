import json
from pymongo import MongoClient
from time import time
from fuzzywuzzy import fuzz,process
import re
from unidecode import unidecode
import sys
import urllib.parse
import requests
from pandas import read_csv
from joblib import Parallel, delayed

from Kahi.KahiParser import KahiParser


# START HELPER FUNCTION SECCTION

def parse_string(text):
    text = unidecode(text.lower())
    text = re.sub( r'[\$_\^]','', re.sub(r'\\\w+','',text ))
    return str(text)

def __colav_similarity(title1,title2,journal1,journal2,year1,year2, ratio_thold=90, partial_thold=95,low_thold=80,verbose=0):

    label = False
    
    #Se revisa si los años y las revistas coinciden
    journal_check = False
    if journal1 and journal2:
        if fuzz.partial_ratio(unidecode(journal1.lower()),unidecode(journal2.lower()))>ratio_thold:
            journal_check=True
    year_check=False
    if year1 and year2:
        if year1==year2:
            year_check = True
    
    length_check=False
    if len(title1.split())>3 and len(title2.split())>3:
        length_check=True
        
    #Si son pocas palabras y no hay por lo menos revista o año para revisar, se descarta de uan vez
    if length_check == False and (journal_check == False or year_check == False):
        return label
    
    if verbose==5:
        if journal_check: print("Journals are the same")
        if year_check: print("Years are the same")
    
    ratio = fuzz.ratio(title1, title2)
    if verbose==5: print("Initial ratio: ",ratio)
    if ratio > ratio_thold and length_check: #Comparación "directa"
        label = True
    if label == False:
        #Comparaciones cuando el título viene en varios idiomas
        title1_list=title1.split("[")
        title2_list=title2.split("[")
        if min([len(item) for item in title1_list]) > 10 and min([len(item) for item in title2_list]) > 10:
            for title in title1_list:
                tmp_title,ratio=process.extractOne(title,title2_list,scorer=fuzz.ratio)
                if ratio > ratio_thold:
                    label=True
                    break
            if verbose==5: print("ratio over list: ",ratio)
            if label==False:
                for title in title1_list:
                    tmp_title,ratio=process.extractOne(title,title2_list,scorer=fuzz.partial_ratio)
                    if ratio > partial_thold:
                        label=True
                        break
                    elif ratio > low_thold:
                        if journal_check and year_check:
                            label=True
                            break
                if verbose==5: print("partial ratio over list: ",ratio)
    
    #Partial ratio section
    if label == False:
        ratio = fuzz.partial_ratio(title1, title2) #Cuando la comparación "directa" falla, relajamos el scorer
        if verbose==5: print("partial ratio: ",ratio)
                
        if ratio > partial_thold and length_check: #si el score supera el umbral (que debería ser mayor al umbral del ratio)
            label=True
        elif ratio > low_thold: #si no lo supera pero sigue siendo un valor alto, revisa el año y la revista
            if journal_check and year_check:
                label=True
             
    return label

def colav_similarity(title1,title2,journal1,journal2,year1,year2,ratio_thold=90, partial_thold=95, low_thold=80,use_regex=True):
    title1 = unidecode(title1.lower())
    title2 = unidecode(title2.lower())
    
    if year1: year1=int(year1)
    if year2: year2=int(year2)
    
    label = False
    
    if not use_regex:
        label = __colav_similarity(title1,title2,journal1,journal2, year1,year2,ratio_thold, partial_thold,low_thold,translation=translate)
    elif use_regex:
        label = __colav_similarity(parse_string(title1), parse_string(title2),journal1,journal2, year1,year2,ratio_thold, partial_thold,low_thold)
    return label
    
#END OF HELPER FUNCTIONS SECTION

class KahiDb(KahiParser):
    def __init__(self,dbserver_url="localhost",port=27017,colav_db="colav",ror_url='https://api.ror.org/organizations?affiliation=',n_jobs=8,verbose=5):
        """
        Base class for Kahi. Includes methods to retrieve, compare, insert and update
        authors, institutions, documents and sources

        ----------------------
        Constructor
        ----------------------
        
        Parameters
        ----------
        dbserver_url : str
            Url for the database server. Default is localhost
        
        port : int
            Port for the database server. Default is 27017
        
        colav_db : str
            Name of the colav database. Default colav

        verbose : int
            Level of developer messages. 0 is no messages and 5 is anoying

        ----------------------
        Methods
        ----------------------
        doct find_source(str):


        """
        super().__init__(verbose=verbose)

        self.ror_url=ror_url
        self.client=MongoClient(dbserver_url,port)
        self.db=self.client[colav_db]

        self.n_jobs=n_jobs
        
        self.db_suffix="antioquia"
        self.collection="stage"

        self.colavdb=self.client["colav_"+self.db_suffix]
        self.griddb=self.client["grid_"+self.db_suffix]
        self.wosdb=self.client["wos_"+self.db_suffix]
        self.lensdb=self.client["lens_"+self.db_suffix]
        #self.scielodb=self.client["scielo_"+self.db_suffix]
        self.scopusdb=self.client["scopus_"+self.db_suffix]
        self.scholardb=self.client["scholar_"+self.db_suffix]
        self.oadoidb=self.client["oadoi_"+self.db_suffix]
        self.doajdb=self.client["doaj"]

        #We have to load in memory all the information needed for the similarity checks
        #Namely: mongo ids, titles, years and sources
        #They have to be dicts since we have to do the similarity check for each raw database
        # but just save the ones that don't have DOI
        self.mongo_ids={"lens":[],"wos":[],"scielo":[],"scopus":[],"scholar":[]}
        self.titles={"lens":[],"wos":[],"scielo":[],"scopus":[],"scholar":[]}
        self.years={"lens":[],"wos":[],"scielo":[],"scopus":[],"scholar":[]}
        self.sources={"lens":[],"wos":[],"scielo":[],"scopus":[],"scholar":[]}

        

        for reg in self.lensdb[self.collection].find():
            doi=""
            for ext in reg["external_ids"]:
                if ext["type"]=="doi":
                    doi=ext["value"]
                    break
            if doi:
                continue
            self.titles["lens"].append(reg["title"])
            journal=""
            if "source" in reg.keys():
                if "title_full" in reg["source"].keys():
                    journal=reg["source"]["title_full"]
            self.sources["lens"].append(str(journal))
            self.years["lens"].append(reg["year_published"])
            self.mongo_ids["lens"].append(reg["_id"])
        
        for reg in self.wosdb[self.collection].find({"doi_idx":""}):
            if reg["doi_idx"]:
                continue
            self.titles["wos"].append(reg["TI"])
            self.sources["wos"].append(str(reg["SO"]))
            year=""
            if reg["PY"]!="":
                try:
                    year=int(reg["PY"])
                except Exception as e:
                    try:
                        year=int(reg["PY"][:-1])
                    except Exception as e:
                        print(e)
            self.years["wos"].append(year)
            self.mongo_ids["wos"].append(reg["_id"])

        for reg in self.scopusdb[self.collection].find({"doi_idx":""}):
            if reg["doi_idx"]:
                continue
            self.titles["scopus"].append(reg["Title"])
            self.sources["scopus"].append(str(reg["Source title"]))
            self.years["scopus"].append(reg["Year"])
            self.mongo_ids["scopus"].append(reg["_id"])

        for reg in self.scholardb[self.collection].find():
            if "doi_idx" in reg.keys():
                if reg["doi_idx"]:
                    continue
            self.titles["scholar"].append(reg["title"])
            self.sources["scholar"].append(str(reg["journal"]))
            year=""
            if reg["year"]!="":
                try:
                    year=int(reg["year"])
                except Exception as e:
                    try:
                        year=int(reg["year"][:-1])
                    except Exception as e:
                        print(e)
            self.years["scholar"].append(year)
            self.mongo_ids["scholar"].append(reg["_id"])

        

    #END __init__

    def find_doaj(self,serials):
        doaj=None
        for serial in serials:
            value=serial["value"]
            doaj=self.doajdb["stage"].find_one({"bibjson.identifier.id":value[:4]+"-"+value[4:]})
            if doaj:
                break
        return doaj

    def find_grid_institution(self,token):
        '''
        Finds the institution through a keyword or name by makig a request to the given URL
        which is the GRID.ac API (local or remote).

        Parameters
        ----------
        token : str
            The key expression to search within the GRID db
        
        Returns
        -------
        result : dict
            GRID.ac result dictionary with the number of results and the full information of them

        '''
        if not token:
            result={}
            result["number_of_results"]=0
            return result
        query=urllib.parse.quote(token)
        url='{}{}'.format(self.ror_url,query)
        res=requests.get(url)
        if res.status_code!=200:
            raise NameError("Server responded with status error: ",res.status_code)
        result={}
        try:
            result=res.json()
        except Exception as e:
            result["number_of_results"]=0
            #print("GOT RESULT: ",result, " BY SEARCHING: ",token)
            #print(e)
        return result
   
    def find_one_doi(self,doi):
        '''
        Uses the DOI to find the accurrences of a document in the different raw databases.

        Parameters
        ----------
        doi : str

        Returns
        -------
        Dict with the raw database name as a key and the register found as its value.
        If the register was not found, the entry is None

        '''
        doi=doi.lower()
        lens_register=None
        wos_register=None
        scielo_register=None
        scopus_register=None
        scholar_register=None
        oadoi_register=None

        register_db=self.db["documents"].find_one({"external_ids.id":doi})
        if register_db:
            return None

        lens_register=self.lensdb[self.collection].find_one({"external_ids.value":doi})
        wos_register=self.wosdb[self.collection].find_one({"doi_idx":doi})
        #scielo_register=self.scielodb[self.collection].find_one({"doi_idx":doi})
        scopus_register=self.scopusdb[self.collection].find_one({"doi_idx":doi})
        scholar_register=self.scholardb[self.collection].find_one({"doi_idx":doi})
        oadoi_register=self.oadoidb[self.collection].find_one({"doi_idx":doi})

        return({"lens":lens_register,
                "wos":wos_register,
                "scielo":scielo_register,
                "scopus":scopus_register,
                "scholar":scholar_register,
                "oadoi":oadoi_register})
    
    def find_many_doi(self,doi_list):
        '''
        Uses a list of DOI to find the accurrences of a document in the different raw databases.

        Parameters
        ----------
        doi_list : list of str

        Returns
        -------
        List of dicts with the raw database name as a key and the register found as its value.
        If the register was not found, the entry is None.


        '''

        register_list=Parallel(n_jobs=self.n_jobs,backend="threading",verbose=10)(delayed(self.find_one_doi)(doi) for doi in doi_list)

        return [reg for reg in register_list if reg]
    

    def find_doi_file(self,file,column):
        '''
        Uses a csv file with a column with the required DOIs to find the accurrences of a document in the different raw databases.

        Parameters
        ----------
        file : str
            path to csv file
        column : str
            Name of the column from which to extract the DOIs

        Returns
        -------
        List of tuples with the found registers in the order: lens, wos, scielo, scopus, scholar, oadoi
        If the register was not found, the entry is None.

        '''
        try:
            data_frame=read_csv(file)
            doi_list=list(data_frame[column])
            register_list=[]
        except:
            print("Could not find the path ",file)
            print(e)
            return 1
        
        register_list=Parallel(n_jobs=self.n_jobs,backend="threading",verbose=10)(delayed(self.find_one_doi)(doi) for doi in doi_list)
        
        return register_list
        
    def evaluate_similarity(self,data,db,i,ratio=90,partial=95,low=80):
        pred = colav_similarity(data["title"],self.titles[db][i],data["source"],self.sources[db][i],data["year"],self.years[db][i],ratio,partial,low,use_regex=True)
        return pred
    
    def parallel_similarity(self,data,db):
        results=Parallel(n_jobs=self.n_jobs,backend="threading",verbose=0)(delayed(self.evaluate_similarity)(data,db,i) for i in range(len(self.titles[db])))
        try:
            idx=results.index(True)
            return idx
        except:
            return None

    def find_one_similarity(self,data):
        '''
        Uses a similarity algorithm to find the corresponding entity in each raw database

        Parameters
        ----------
        data : dict
            Dictionary with the keys: "title", "source", "year". Which is the information needed to
            use the similarity algorithm. If one of those fields is not present, use epty string.

        Returns
        -------
        Tuple with the found registers in the order: lens, wos, scielo, scopus, scholar, oadoi
        If the register was not found, the entry is None.

        '''
        registers=[]
        for db in ["lens","wos","scielo","scopus","scholar"]:#,"oadoi"]:
            idx=self.parallel_similarity(data,db)
            if idx or idx==0:
                mongo_id=self.mongo_ids[db][idx]
                if db=="lens":
                    reg=self.lensdb[self.collection].find_one({"_id":mongo_id})
                    registers.append(reg)
                if db=="wos":
                    reg=self.wosdb[self.collection].find_one({"_id":mongo_id})
                    registers.append(reg)
                if db=="scielo":
                    reg=self.scielodb[self.collection].find_one({"_id":mongo_id})
                    registers.append(reg)
                if db=="scopus":
                    reg=self.scopusdb[self.collection].find_one({"_id":mongo_id})
                    registers.append(reg)
                if db=="scholar":
                    reg=self.scholardb[self.collection].find_one({"_id":mongo_id})
                    registers.append(reg)

            else:
                registers.append(None)
        return tuple(registers)

    def find_many_similarity(self,data_list):
        '''
        Uses a similarity algorithm to find the corresponding entity in each raw database

        Parameters
        ----------
        data_list : list
            List of dictionaries with the keys: "title", "source", "year". Which is the information needed to
            use the similarity algorithm. If one of those fields is not present, use epty string.

        Returns
        -------
        List of tuples with the found registers in the order: lens, wos, scielo, scopus, scholar, oadoi
        If the register was not found, the entry is None.

        '''
        register_list=[]
        for data in data_list:
            register_list.append(find_one_similarity(data))
        return register_list

    def find_from_collection(self,db,collection,field):
        '''
        Search for the entities given a dataset in a mongo db collection.
        Dataset must have the keys: doi, title, source and year.
        The function searches the corresponding entity in the raw databases
        even if the doi is not provided.

        Parameters
        ----------
        db : str
            Name of the mongodb database
        collection : str
            Name of the collection inside db which contains the data

        Returns
        -------
        List of tuples with the found registers in the order: lens, wos, scielo, scopus, scholar, oadoi
        If the register was not found, the entry is None.

        '''
        register_list=[]
        for register in self.client[db][collection].find():
            doi=""
            if field in register.keys():
                if register[field]:
                    doi=register[field]
            if doi:
                register_list.append(self.find_one_doi(doi))
            else:
                register_list.append(self.find_one_similarity(register))
        return register_list

    def get_doilist_from_collection(self,db,collection,field):
        doilist=[]
        for reg in self.client[db][collection].find({},{field:1}):
            regdb=self.db["documents"].find_one({"external_ids.id":reg[field]})
            if not regdb:
                doilist.append(reg[field])
        return doilist
            
    def link_authors_institutions(self,author_institution):
        '''
        Searches for the author and affiliations in CoLav database.
        TODO:
            Check if the ids should be lowercase. Right now there is no standard.

        Parameters
        ----------
        author_institution : dict
            Portion of document register with one author and his/hers affiliations

        Returns
        -------
        author_institution : dict
            Modified register with the ids found for author and his/hers affiliations.
            If the author or the institutions were not found, the register is not modified

        '''
        author_found=False
        author=None
        for id_ext in author_institution["external_ids"]:
            idx=id_ext["value"]
            author=self.db["authors"].find_one({"external_ids.value":idx})
            if author:
                author_found=True
                break
        if author_found==False: #search through aliases
            for alias in author_institution["aliases"]:
                author=self.db["authors"].find_one({"aliases":alias})
                if author:
                    author_found=True
                    break
        if author_found:
            if self.verbose==5: print("Found ",author["full_name"])
            author_institution["_id"]=author["_id"]
            #author modifications section
            mod={}
            if not author_institution["first_names"] and author["first_names"]:
                mod["first_names"]=author["first_names"]
            if not author_institution["last_names"] and author["last_names"]:
                mod["last_names"]=author["last_names"]
            if not author_institution["initials"] and author["initials"]:
                mod["initials"]=author["initials"]
            alias_mod=author["aliases"]
            alias_modified=False
            for alias in author_institution["aliases"]:
                if not alias in alias_mod:
                    alias_mod.append(alias)
                    alias_modified=True
            if alias_modified:
                mod["aliases"]=alias_mod
            keywords_mod=author["keywords"]
            keywords_modified=False
            for keyword in author_institution["keywords"]:
                if not keyword in keywords_mod:
                    keywords_mod.append(keyword)
                    keywords_modified=True
            if keywords_modified:
                mod["keywords"]=keywords_mod
            ids_mod=author["external_ids"]
            ids_modified=False
            for idx in author_institution["external_ids"]:
                found=False
                for reg_idx in author["external_ids"]:
                    if reg_idx["value"]==idx["value"]:
                        found=True
                        # if the values are equal, check if the type is equal to update it if needed
                        if reg_idx["source"]==idx["source"]:
                            if not idx in ids_mod:
                                ids_mod.append(idx)
                            break
                        elif idx["source"]!="unknown":
                            entry={"source":idx["source"],"value":idx["value"]}
                            if not entry in  ids_mod:
                                ids_mod.append(entry)
                                ids_modified=True
                            break
                if found==False:
                    if not idx in ids_mod:
                        ids_mod.append(idx)
                        ids_modified=True
            if ids_modified:
                mod["external_ids"]= ids_mod
            if mod:
                mod["updated"]=int(time())
                author_institution["mod"]=mod
            #END MODIFICATIONS SECTION

        
        #Searching for institutions
        for affiliation in author_institution["affiliations"]:
            if self.verbose==5: print("Searching ",affiliation["name"])
            aff_found=False
            affdb=None
            for id_ext in affiliation["external_ids"]: #try external_ids
                idx=id_ext["value"]
                affdb=self.db["institutions"].find_one({"external_ids.value":idx})
                if affdb:
                    aff_found=True
                    break
            if aff_found==False: #Try grid method
                response=self.find_grid_institution(affiliation["name"])
                if response["number_of_results"]!=0:
                    if response["items"][0]["score"]>0.8:
                        gridid=response["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                        affdb=self.db["institutions"].find_one({"external_ids.value":gridid})
                        if affdb:
                            if "redirect" in affdb.keys():
                                affdb=self.db["institutions"].find_one({"external_ids.value":affdb["redirect"]})
                                aff_found=True
                                break
            if aff_found:
                if self.verbose==5: print("Affiliation found in institutions collection")
                affiliation["_id"]=affdb["_id"]
                #affiliations modification section
                mod={}
                #if not affiliation[""] and affdb[""]:
                #    mod[""]=affdb[""]
                alias_mod=affdb["aliases"]
                alias_modified=False
                for alias in affiliation["aliases"]:
                    if not alias in alias_mod:
                        alias_mod.append(alias)
                        alias_modified=True
                if alias_modified:
                    mod["aliases"]=alias_mod
                abb_mod=affdb["abbreviations"]
                abb_modified=False
                for abb in affiliation["abbreviations"]:
                    if not abb in abb_mod:
                        abb_mod.append(abb)
                        abb_modified=True
                if abb_modified:
                    mod["abbreviations"]=abb_mod
                types_mod=affdb["types"]
                types_modified=False
                for tp in affiliation["types"]:
                    if not tp in types_mod:
                        typess_mod.append(tp)
                        types_modified=True
                if types_modified:
                    mod["types"]=types_mod

                ids_mod=affdb["external_ids"]
                ids_modified=False
                for idx in affiliation["external_ids"]:
                    found=False
                    for reg_idx in affdb["external_ids"]:
                        if reg_idx["value"]==idx["value"]:
                            found=True
                            # if the values are equal, check if the type is equal to update it if needed
                            if reg_idx["source"]==idx["source"]:
                                if not idx in ids_mod:
                                    ids_mod.append(idx)
                                break
                            elif idx["source"]!="unknown":
                                entry={"source":idx["source"],"value":idx["value"]}
                                if not entry in  ids_mod:
                                    ids_mod.append(entry)
                                    ids_modified=True
                                break
                    if found==False:
                        if not idx in ids_mod:
                            ids_mod.append(idx)
                            ids_modified=True
                if ids_modified:
                    mod["external_ids"]= ids_mod

                url_mod=affdb["external_ids"]
                url_modified=False
                for idx in affiliation["external_urls"]:
                    found=False
                    for reg_idx in affdb["external_ids"]:
                        if reg_idx["value"]==idx["value"]:
                            found=True
                            # if the values are equal, check if the type is equal to update it if needed
                            if reg_idx["source"]==idx["source"]:
                                if not idx in url_mod:
                                    ids_mod.append(idx)
                                break
                            elif idx["source"]!="unknown":
                                entry={"source":idx["source"],"value":idx["value"]}
                                if not entry in  url_mod:
                                    url_mod.append(entry)
                                    url_modified=True
                                break
                    if found==False:
                        if not idx in url_mod:
                            url_mod.append(idx)
                            url_modified=True
                if url_modified:
                    mod["external_ids"]= url_mod
                
                if mod:
                    mod["updated"]=int(time())
                    affiliation["mod"]=mod
                #END MODIFICATION SECTION
            

        return author_institution
                        
    def link_source(self,source):
        '''
        Find the source (right now only the journal) in the colav database

        Parameters
        ----------
        source : dict
            Full parsed and joined register
        
        Returns
        -------
        register : dict
            The complete register of the source in colav db with the _id and modifications to be made.
            If no register was found, returns the unmodified input register   

        '''
        register=None
        for serial in source["serials"]:
            register=self.db["sources"].find_one({"serials.value":serial["value"]})
            if register:
                break
        if register:
            source["_id"]=register["_id"]
            mod={}
            if not register["type"] and source["type"]:
                mod["type"]=source["type"]
            if not register["institution"] and source["institution"]:
                mod["institution"]=source["institution"]
            if not "institution_id" in register.keys():
                if (source["institution"] or register["institution"]):
                    if register["institution"]:
                        institution=register["institution"]
                    elif source["institution"]:
                        institution=source["institution"]
                    response=self.find_grid_institution(institution)
                    if response["number_of_results"]!=0:
                        if response["items"][0]["score"]>0.8:
                            gridid=response["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                            affdb=self.db["institutions"].find_one({"external_ids.value":gridid})
                            if affdb:
                                mod["institution"]=affdb["name"]
                                mod["institution_id"]=affdb["_id"]
            else:
                if not register["institution_id"] and (source["institution"] or register["institution"]):
                    if register["institution"]:
                        institution=register["institution"]
                    elif source["institution"]:
                        institution=source["institution"]
                    response=self.find_grid_institution(institution)
                    if response["number_of_results"]!=0:
                        if response["items"][0]["score"]>0.8:
                            gridid=response["items"][0]["organization"]["external_ids"]["GRID"]["preferred"]
                            affdb=self.db["institutions"].find_one({"external_ids.value":gridid})
                            if affdb:
                                mod["institution"]=affdb["name"]
                                mod["institution_id"]=affdb["_id"]
                
            if not register["publisher"] and source["publisher"]:
                mod["publisher"]=source["publisher"]
                mod["publisher_idx"]=source["publisher_idx"]
            if not register["country"] and source["country"]:
                mod["country"]=source["country"]
            #serials
            serials_mod=register["serials"]
            serials_modified=False
            for serial in source["serials"]:
                found=False
                for reg_serial in register["serials"]:
                    if reg_serial["value"]==serial["value"]:
                        found=True
                        # if the values are equal, check if the type is equal to update it if needed
                        if reg_serial["type"]==serial["type"]:
                            if not serial in serials_mod:
                                serials_mod.append(serial)
                            break
                        elif serial["type"]!="unknown":
                            entry={"type":serial["type"],"value":serial["value"]}
                            if not entry in  serials_mod:
                                serials_mod.append(entry)
                            serials_modified=True
                            break
                if found==False:
                    if not serial in  serials_mod:
                        serials_mod.append(serial)
                    serials_modified=True
            #abbreviations
            abb_mod=register["abbreviations"]
            abb_modified=False
            for abb in source["abbreviations"]:
                found=False
                for reg_abb in register["abbreviations"]:
                    if reg_abb["value"]==abb["value"]:
                        found=True
                        # if the values are equal, check if the type is equal to update it if needed
                        if reg_abb["type"]==abb["type"]:
                            if not abb in abb_mod:
                                abb_mod.append(abb)
                            break
                        elif abb["type"]!="unknown":
                            entry={"type":abb["type"],"value":abb["value"]}
                            if not entry in  abb_mod:
                                abb_mod.append(entry)
                            abb_modified=True
                            break
                if found==False:
                    if not abb in  abb_mod:
                        abb_mod.append(abb)
                    abb_modified=True
            if abb_modified:
                mod["abbreviations"]=abb_mod
            if serials_modified:
                mod["serials"]=serials_mod
            if mod:
                mod["source_checked"]=register["source_checked"]
                for source_checked in source["source_checked"]:
                    found=False
                    for reg_checked in mod["source_checked"]:
                        if source_checked["source"]==reg_checked["source"]:
                            found=True
                            if source_checked["date"]>reg_checked["date"]:
                                reg_checked["date"]=source_checked["date"]
                    if found==False:
                        mod["source_checked"].append(source_checked)
                mod["updated"]=int(time())
                source["mod"]=mod
        return source
    
    
    def insert_one(self,register):
        #Source section
        if "_id" in register["source"].keys():
            if "mod" in register["source"].keys():
                response=self.db["sources"].update_one({"_id":register["source"]["_id"]},{"$set":register["source"]["mod"]})
        else:
            result=self.db["sources"].insert_one(register["source"])
            register["source"]["_id"]=result.inserted_id
        #removing all information but the id
        source_id=register["source"]["_id"]
        register["source"]={"_id":source_id}

        #author and affiliations section
        authors=[]
        for author in register["author_institutions"]:
            affiliations=[]
            for aff in author["affiliations"]:
                if "_id" in aff.keys(): #modify whats is needed
                    if "mod" in aff.keys():
                        result=self.db["institutions"].update_one({"_id":aff["_id"]},{"$set":aff["mod"]})
                else:#insert the affiliation and recover the id
                    result=self.db["institutions"].insert_one(aff)
                    aff["_id"]=result.inserted_id
                #removing all information but the id
                mongo_id=aff["_id"]
                aff={"_id":mongo_id}
                affiliations.append(aff)
            if "_id" in author.keys(): #modify what's needed
                if "mod" in author.keys():
                    result=self.db["authors"].update_one({"_id":author["_id"]},{"$set":author["mod"]})
            else: #insert the author and recover the id
                del(author["affiliations"])
                result=self.db["authors"].insert_one(author)
                author["_id"]=result.inserted_id
            #removing all information but the id
            author_id=author["_id"]
            author={"_id":author_id,"affiliations":affiliations,"corresponding":author["corresponding"]}
            authors.append(author)

        register["author_institutions"]=authors
        
        #Building the complete document register
        register["document"]["source"]=register["source"]
        register["document"]["authors"]=register["author_institutions"]
        result=self.db["documents"].insert_one(register["document"])
        return result

    def update_many(self,registry_list):
        pass