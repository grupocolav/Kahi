import json
from pymongo import MongoClient
from time import time
from fuzzywuzzy import fuzz,process
import re
from unidecode import unidecode
import sys
from pandas import read_csv
from joblib import Parallel, delayed

from Kahi.KahiDb import KahiDb




class Kahi(KahiDb):
    def __init__(self,dbserver_url="localhost",port=27017,colav_db="colav",n_jobs=8,verbose=0):
        '''
        Class with the attributes and methods that will put the entire ETL process together
        '''
        super().__init__(dbserver_url=dbserver_url,port=port,colav_db=colav_db,n_jobs=n_jobs,verbose=verbose)
        
        self.articles_doi=[] #articles with dois
        self.articles=[] #articles without dois
        self.transformed=[] #articles in CoLav format
        self.loaded=[] #link to the ids of the loaded registers {document,author,institution,source}
        self.status={} #Marks the status of the process. For example: data cannot be loaded if its not linked
    
    #there should be different options to extract from files, lists or collections in mongo
    def extract_doi(self,data):
        '''
        Extracts the articles from the different raw sources in the database given a list of dois

        Parameters
        ----------
        data : list
            List of dois
        '''
        self.articles_doi.extend(self.find_many_doi(data))

    def extract_from_doi_file(self,file,column):
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
        If the register was not found, the entry is None

        '''
        self.articles_doi.extend(self.find_doi_file(file,column))

    def extract_similarity(self,data):
        '''
        Extracts articles using a similarity algorithm
        to find the corresponding entity in each raw database

        Parameters
        ----------
        data : list
            List of dictionaries with the keys: "title", "source", "year". Which is the information needed to
            use the similarity algorithm. If one of those fields is not present, use epty string.
        '''
        self.articles.extend(self.find_many_similarity(data))

    def extract_from_collection(self,db,collection):
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
        self.articles.extend(self.find_from_collection(db,collection))


    def transform(self):
        '''
        Transforms the data extracted in CoLav's format
        '''
        parsed=[]
        for paper in self.articles_doi:
            entry={}
            entry["document"]=self.parse_document(paper)
            entry["author_institutions"]=self.parse_authors_institutions(paper)
            entry["source"]=self.parse_source(paper)
            parsed.append(entry)
        for paper in self.articles:
            entry={}
            entry["document"]=self.parse_document(paper)
            entry["author_institutions"]=self.parse_authors_institutions(paper)
            entry["source"]=self.parse_source(paper)
            parsed.append(entry)
        for paper in parsed:
            entry={}
            entry["document"]=self.join_document(paper["document"])
            entry["author_institutions"]=self.join_authors_institutions(paper["author_institutions"])
            entry["source"]=self.join_source(paper["source"])
            self.transformed.append(entry)


    def link(self):
        '''
        Links the transformed data to existing registers in the database
        '''
        linked=[]
        for paper in self.transformed:
            entry={}
            entry["author_institutions"]=[]
            entry["document"]=paper["document"]
            for author in entry["author_institutions"]:
                entry["author_institutions"].append(self.link_authors_institutions(author))
            entry["source"]=self.link_source(paper["source"])
            linked.append(entry)
        self.transformed=linked

    def load(self):
        '''
        Loads the new registers to the databse and modify them if needed.
        '''
        for paper in self.transformed:
            #Source section
            if "_id" in paper["source"].keys():
                if "mod" in paper["source"].keys():
                    response=self.db["sources"].update_one({"_id":paper["source"]["_id"]},{"$set":paper["source"]["mod"]})
            else:
                result=self.db["sources"].insert_one(paper["source"])
                paper["source"]["_id"]=result.inserted_id
            #removing all information but the id
            source_id=paper["source"]["_id"]
            paper["source"]={"_id":source_id}

            #author and affiliations section
            for author in paper["author_institutions"]:
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
                affiliations=author["affiliations"]
                if "_id" in author.keys(): #modify what's needed
                    if "mod" in author.keys():
                        result=self.db["authors"].update_one({"_id":author["_id"]},{"$set":author["mod"]})
                else: #insert the author and recover the id
                    del(author["affiliations"])
                    result=self.db["authors"].insert_one(author)
                    author["_id"]=result.inserted_id
                #removing all information but the id
                author_id=author["_id"]
                author={"_id":author_id,"affiliations":affiliations}
            
            #Building the complete document register
            paper["document"]["source"]=paper["source"]
            paper["document"]["authors"]=paper["author_institutions"]
            result=self.db["documents"].insert_one(paper["document"])
                
