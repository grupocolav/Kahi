from elasticsearch import Elasticsearch
from pymongo import MongoClient
import sys

class Queries:
    def __init__(self,db_name="MAG",db_ip="127.0.0.1",db_port=27017,db_es_index="mag",db_es_ip="127.0.0.1",db_es_port=9200,db_es_timeout=60):
        """
        Class to get information of Microsoft Academic Graph from MongoDB and Elastic Search.
        Allows to get paper by Title on ElasticSearch and Paper by Doi and ID using MongoDB.

        The dataset must be previously loaded with Inti on the MongoDB and ElasticSearch databases.
        """
        self.client = MongoClient(db_ip,db_port)
        self.db_name = db_name
        self.db = self.client[db_name]
        self.es = Elasticsearch(HOST="http://{}".format(db_es_ip), PORT=db_es_port,timeout=db_es_timeout)
        self.db_es_index = db_es_index
        if not self.es.indices.exists(db_es_index):
            print("Error: Elastic search index {} doesn't exists".format(db_es_index))
            sys.exit(1)
            

    def get_papers_by_title(self,title,limit=10):
        """
        Perform the search by Title on ElasticSearch

        Parameters
        ----------
        title : string
           Paper title
        
        Returns
        -------
        result : array  
            documents found on Elastic Search

        """
        body = {
                "from":0,
                "size":limit,
                "query": {
                    "match": {
                        "PaperTitle":title
                    }
                }
            }
        result = self.es.search(index=self.db_es_index, body=body)
        return result
        
    def get_paper_by_doi(self,doi):
        """
        Perform the search by Doi on MongoDB

        Parameters
        ----------
        doi : string
           Paper Doi
        
        Returns
        -------
        result : Dict 
            document found on MongoDB performing the relations between the collections

        """
        paper = self.db['Papers'].find_one({'Doi': doi},{'_id':0})
        if not paper:
            return None
        
        pid = paper['PaperId']
        return self.get_paper_by_id(pid)

    def get_paper_by_id(self,pid):
        """
        Perform the search by MA Paper ID on MongoDB

        Parameters
        ----------
        pid : long
           Microsot Paper ID
        
        Returns
        -------
        result : Dict 
            document found on MongoDB performing the relations between the collections

        """
        pid=int(pid)
        db = self.db
        paper = {}
        _paper = db['Papers'].find_one({'PaperId': pid},{'_id':0})
        if _paper is None:
            print("=== Error: Paper not found, on db = {} with pid = {}".format(self.db_name,pid))
            return paper
        paper['Paper'] = _paper
         
    
        ###MAG
        paper['PaperExtendedAttributes'] = list(db['PaperExtendedAttributes'].find({'PaperId': pid},{'_id':0}))
        paper['PaperResources'] = list(db['PaperResources'].find({'PaperId': pid},{'_id':0}))
        paper['Journal'] = db['Journals'].find_one({'JournalId': paper['Paper']['JournalId']},{'_id':0})
        paper['PaperUrls'] = list(db['PaperUrls'].find({'PaperId': pid},{'_id':0}))
        
        if paper['Paper']['ConferenceSeriesId'] != '':
            paper['ConferenceSeries'] = list(db['ConferenceSeries'].find({'ConferenceSeriesId': paper['Paper']['ConferenceSeriesId']},{'_id':0}))
        else:
            paper['ConferenceSeries'] = []
            
        if paper['Paper']['ConferenceInstanceId'] != '':
            paper['ConferenceInstance'] = list(db['ConferenceInstances'].find({'ConferenceInstanceId': paper['Paper']['ConferenceInstanceId']},{'_id':0}))
        else:
            paper['ConferenceInstance'] = []
        
        paper['Authors'] = []
        
        
        authors_aff = list(db['PaperAuthorAffiliations'].find({'PaperId': pid},{'_id':0}))
        
        for author_aff in authors_aff:
            aid = author_aff['AuthorId']
            affid =  author_aff['AffiliationId']
            
            author = {}
            author['Author'] = list(db["Authors"].find({'AuthorId': aid},{'_id':0}))
            author['PaperAuthorAffiliations'] = author_aff
            #author['AuthorExtendedAttributes'] = list(db['AuthorExtendedAttributes'].find({'AuthorId': aid},{'_id':0})) #required updated mag data version
            author['Affiliation'] = list(db['Affiliations'].find({'AffiliationId': affid},{'_id':0}))
            paper['Authors'].append(author)
        ##NLP
        paper['PaperAbstractsInvertedIndex'] = list(db['PaperAbstractsInvertedIndex'].find({'PaperId': pid},{'_id':0}))
    
        #MAG
        paper['PaperReferences'] =  list(db['PaperReferences'].find({'PaperId': pid},{'_id':0}))
        
        ##NLP
        paper['PaperCitationContexts'] = list(db['PaperCitationContexts'].find({'PaperId': pid},{"_id":0}))
        ##ADVANCED
        paper['PaperFieldsOfStudy'] =  list(db['PaperFieldsOfStudy'].find({'PaperId': pid},{"_id":0}))
        paper['FieldsOfStudy'] = []
        for pfstudy in paper['PaperFieldsOfStudy']:
            fosid = pfstudy['FieldOfStudyId']
            field = {}
            field['FieldOfStudy'] = list(db['FieldsOfStudy'].find({'FieldOfStudyId':fosid },{'_id':0}))
            field['FieldOfStudyExtendedAttributes'] = list(db['FieldOfStudyExtendedAttributes'].find({'FieldOfStudyId': fosid},{'_id':0}))
            field['FieldOfStudyChildren'] = list(db['FieldOfStudyChildren'].find({'FieldOfStudyId': fosid},{'_id':0}))
            field['RelatedFieldOfStudy'] = list(db['FieldOfStudyChildren'].find({'FieldOfStudyId1': fosid},{'_id':0}))
            paper['FieldsOfStudy'].append(field)
            #no clear RelatedFieldOfStudy  
        paper['PaperRecommendations'] = list(db['PaperRecommendations'].find({'PaperId': pid},{"_id":0}))
        return paper
