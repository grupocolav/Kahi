from pymongo import MongoClient

#TODO:
# Create an algoroithm to search authors, institutions and sources
# User case: the search somewhat returns more than one document, error handling


class KahiBase():
    def __init__(self,dbserver_url="localhost",port=27017,colav_db="colav",verbose=5):
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
        bool register_compare(register1,register2)
        document check_doi(doi)
        (exist,document) check_document(document,doi)


        """
        self.client=MongoClient(dbserver_url,port)
        self.db=self.client[colav_db]
        self.verbose=verbose

    def register_compare(self,reg1,reg2):
        """
        Compare two registers in CoLav format.
        They could be of any collection, i.e. documents, authors, institutions, sources

        Parameters
        ----------
        reg1 : dict
            Register in CoLav format
        
        reg2 : dict
            Register in CoLav format

        Returns
        -------
        equal : bool
            True if the registers are exactly the same
            False otherwise

        """
        if "_id" in reg1.keys():
            del(reg1["_id"])
        if "_id" in reg2.keys():
            del(reg2["_id"])

        return reg1==reg2

    def check_doi(self,doi):
        """
        Checks through the doi identifier if a document exists in the db

        Parameters
        ----------
        doi : str
            Document online identifier
        
        Returns
        -------
        document : dict
             None if the document was not found in the db.
             The complete register otherwise

        """
        if self.verbose==5: print("DOI provided. Searching a document")
        response=self.db["documents"].find_one({"external_ids.value":doi})
        return response
        

    def check_document(self,document=None,doi=""):
        """
        Checks if the document is in the database and
        if it has to be updated, i.e. If the information
        of the document provided is included in the document
        already saved in the db.
        
        Parameters
        ----------
        document : dict
            Document in CoLav standard format.
        
        doi : str
            Online document identifier

        Returns
        -------
        exist : int 
            -1 if  the document was not found,
            1 if the document was found and updatable
            and 0 otherwise.

        document: dict 
            None if the document was not found,
            the outdate document entry in the db if the document was found and is updatable
            and the full document entry in the db otherwise.

        """
        response=None
        if not doi and not document:
            if self.verbose==5: print("No arguments were provided")
            return (-1,response)
        elif doi:
            if self.verbose==5: print("DOI provided. Searching a document")
            # Check if the doi exists then compare it with the document provided
            response=self.db["documents"].find_one({"external_ids.value":doi})
            if response:
                if not document: #If the document os not provided return 0 and the response
                    return (0,response)
                else: #If the document was provided check for equality and return accordingly
                    if self.register_compare(document,response):
                        return (0,response)
                    else:
                        return (1,response)        
            else: #if there is no response in the db
                return (-1,response)
        elif document and not doi:
            if self.__verbose==5: print("Document provided. Searching...")
            # Check by doi first, then by any identifier in the external ids in the document provided
            # Until the external ids list is depleated
            # (algorithm of similarity must change)
            # Then compare the document provided with the db entry until a field is not equal
            if "external_ids" in document.keys():
                for idx,source in enumerate([reg["source"] for reg in document["external_ids"]]):
                    response=self.db["documents"].find_one({"external_ids.value":document["external_ids"][idx]["value"]})
                    if response:
                        if self.register_compare(document,response):
                            return (0,response)
                        else:
                            return (1,response)
                if not response:
                    return (-1,response)
                            


    def check_author(self,author):
        """
        Checks if the author is in the database and
        if it has to be updated, i.e. If the information
        of the uathor provided is included in the author
        already saved in the db.
        
        Parameters
        ----------
        author : dict
            Author in CoLav standard format.

        Returns
        -------
        exist : int 
            -1 if  the author was not found, 1 if the author was found and updatable
            and 0 otherwise.

        author: dict 
            empty dict if the author was not found, the outdate author entry in the db if the author was found and is updatable
            and the full author entry in the db otherwise.

        """
        if "external_ids" in author.keys():
            if "external_ids":
                response=None
                for idx,source in enumerate([reg["source"] for reg in author["external_ids"]]):
                    response=self.db["authors"].find_one({"external_ids.value":author["external_ids"][idx]["value"]})
                    if response:
                        if self.register_compare(author,response):
                            return (0,response)
                        else:
                            return (1,response)
                if not response:
                    return (-1,response)

    
    def update_author(self,author):
        """
        Updates author with the new information or add a new
        registry to the database if it is not.

        Parameters
        ----------

        author : dict
            Author in CoLav standard format

        Returns
        -------
        author : dict
            Database entry for the updated author 
        """
        pass

    def check_institution(self):
        pass

    def update_institution(self):
        pass

    def check_source(self):
        pass

    def update_source(self):
        pass