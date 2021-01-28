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
            #entry["document"]=self.link_document(paper["document"])
            entry["author_institutions"]=self.link_authors_institutions(paper["author_institutions"])
            entry["source"]=self.link_source(paper["source"])
        self.transformed=linked

    def load(self):
        '''
        Loads the new registers (if needed) to the database
        '''
        pass
