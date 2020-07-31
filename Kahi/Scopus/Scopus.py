from time import time
from datetime import datetime as dt
import iso3166
import iso639
from fuzzywuzzy import fuzz,process

class Scopus():
    def __init__(self):
        '''
        Base class to parse Lens data into colva standard
        '''
        pass

    def parse_document(self, reg):
        """
        Transforms the raw register document information from Scopus in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Scopus format
        
        Returns
        -------
        document : dict
            Information of the document in the CoLav standard format
        """
        data={}
        data["updated"]=int(time())

        data["publication_type"]=""

        if "Title" in reg.keys():
            data["title"]=reg["Title"]
        else:
            data["title"]=""
        if "Year" in reg.keys(): data["year_published"]=reg["Year"]
        else: data["year_published"]=""
        if "Volume" in reg.keys(): 
            if reg["Volume"] and reg["Volume"]==reg["Volume"]:
                data["volume"]=reg["Volume"]
            else:
                data["volume"]=""
        else:
            data["volume"]=""
        if "Issue" in reg.keys():
            if reg["Issue"] and reg["Issue"]==reg["Issue"]:
                data["issue"]=reg["Issue"]
            else:
                data["issue"]=""
        else:
            data["issue"]=""
        if "Page start" in reg.keys():
            if reg["Page start"] and reg["Page start"] == reg["Page start"]: #checking for NaN in the second criteria
                try:
                    data["start_page"]=int(reg["Page start"])
                except:
                    data["start_page"]=""
            else:
                data["start_page"]=""
        else:
            data["start_page"]=""
        if "Page end" in reg.keys():
            if reg["Page end"] and reg["Page end"] == reg["Page end"]:
                try:
                    data["end_page"]=int(reg["Page end"])
                except:
                    data["end_page"]=""
            else:
                data["end_page"]=""
        else:
            data["end_page"]=""
        if "Abstract" in reg.keys():
            if reg["Abstract"] and reg["Abstract"]==reg["Abstract"]:
                data["abstract"]=reg["Abstract"]
            else:
                data["abstract"]=""
        else:
            data["abstract"]=""
        if "Document type" in reg.keys():
            data["publication_type"]=reg["Document type"].lower()
            if data["publication_type"]=="conference paper":
                if "Conference name" in reg.keys():
                    data["conference_name"]=reg["Conference name"]
                if "Conference date" in reg.keys():
                    data["conference_date"]=reg["Conference date"]
                if "Conference location" in reg.keys():
                    data["conference_location"]=reg["Conference location"]
                if "Conference code" in reg.keys():
                    data["conference_code"]=reg["Conference code"]
        if "Language of Original Document" in reg.keys():
            if reg["Language of Original Document"] and reg["Language of Original Document"]==reg["Language of Original Document"]:
                lang=reg["Language of Original Document"]
                langs=lang.split("; ")
                data["languages"]=[]
                for lang in langs:
                    if lang == 'Irish Gaelic':
                        data["languages"].append("gd")
                    elif lang == 'Gallegan':
                        data["languages"].append("es")
                    else:
                        data["languages"].append(iso639.languages.get(name=lang).part1)
        if "Index Keywords" in reg.keys():
            if reg["Index Keywords"] and reg["Index Keywords"] == reg["Index Keywords"]:
                data["keywords"]=reg["Index Keywords"].lower().split("; ")

        if "Funding Details" in reg.keys():
            if reg["Funding Details"] and reg["Funding Details"] == reg["Funding Details"]:
                data["funding_organization"]=reg["Funding Details"]
            else:
                data["funding_organization"]=""
        else:
            data["funding_organization"]=""
        for i in range(1,5):
            texts=[]
            if "Funding Text "+str(i) in reg.keys():
                if reg["Funding Text "+str(i)] and reg["Funding Text "+str(i)] == reg["Funding Text "+str(i)]:
                    texts.append(reg["Funding Text "+str(i)])
            if len(texts) != 0:
                data["funding_details"]=texts
            else:
                data["funding_details"]=""
                
        if "Access Type" in reg.keys():
            if reg["Access Type"] and reg["Access Type"] == reg["Access Type"]:
                data["access_type"]=reg["Access Type"]
        
        data["external_ids"]=[]
        if "DOI" in reg.keys(): data["external_ids"].append({"source":"doi","id":reg["DOI"]})
        if "EID" in reg.keys(): data["external_ids"].append({"source":"scopus","id":reg["EID"]})
        if "Pubmed ID" in reg.keys(): data["external_ids"].append({"source":"pubmed","id":reg["Pubmed ID"]})

        if "Authors" in reg.keys():
            if reg["Authors"] and reg["Authors"]==reg["Authors"]:
                data["author_count"]=len(reg["Authors"].split(", "))
            else:
                data["author_count"]=""
        else:
            data["author_count"]=""


        data["urls"]=[]
        if "Link" in reg.keys(): data["urls"].append({"source":"scopus","url":reg["Link"]})

        #REFERENCES SECTION
        if "References" in reg.keys():
            if reg["References"] and reg["References"]==reg["References"]:
                references=reg["References"].split("; ")
                data["references_count"]=len(references)
            else:
                data["references_count"]=""
        else:
            data["references_count"]=""

        #CITATION SECTION
        if "Cited by" in reg.keys():
            if reg["Cited by"] and reg["Cited by"]==reg["Cited by"]:
                data["citations_count"]=int(reg["Cited by"])
            else:
                data["citations_count"]=""
        else:
            data["citations_count"]=""

        return data

    def parse_authors(self,reg):
        """
        Transforms the raw register author information from Scopus in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Scopus format
        
        Returns
        -------
        authors : list
            Information of the authors in the CoLav standard format
        """
        authors=[]
        ids=None
        corresponding_author=""
        corresponding_address=""
        corresponding_email=""
        if "Correspondence Address" in reg.keys():
            if reg["Correspondence Address"] and reg["Correspondence Address"]==reg["Correspondence Address"]:
                corresponding_list=reg["Correspondence Address"].split(";")
                #print(corresponding_list)
                if len(corresponding_list)>0: corresponding_author=corresponding_list[0]
                if len(corresponding_list)>1: corresponding_address=corresponding_list[1]
                if len(corresponding_list)>2: corresponding_email=corresponding_list[2]
        if "Authors" in reg.keys():
            if reg["Authors"] and reg["Authors"]==reg["Authors"]:
                if "Author(s) ID" in reg.keys(): ids=reg["Author(s) ID"].split(";")
                for idx,author in enumerate(reg["Authors"].split(", ")):
                    try:
                        entry={"full_name":author,
                        "last_names":author.split(" ")[0],
                        "initials":author.split(" ")[1].replace(".","")}
                    except:
                        continue
                    #print("\n")
                    #print(author,corresponding_author)
                    #print("\n")
                    if corresponding_author:
                        if author==corresponding_author.replace(",",""):
                            entry["corresponding"]=True
                            entry["corresponding_address"]=corresponding_address
                            entry["corresponding_email"]=corresponding_email.replace("email: ","")
                    if ids:
                        try:
                            entry["external_ids"]=[{"source":"scopus","value":ids[idx]}]
                        except:
                            continue
                    authors.append(entry)


        return authors

    def parse_institutions(self,reg):
        """
        Transforms the raw register institution informatio from Scopus in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Scopus format
        
        Returns
        -------
        institutions : list
            Information of the institutions in the CoLav standard format
        """
        inst=[]
        country_list=list(iso3166.countries_by_name.keys())
        if "Authors with affiliations" in reg.keys():
            if reg["Authors with affiliations"] and reg["Authors with affiliations"]==reg["Authors with affiliations"]:
                auwaf_list=reg["Authors with affiliations"].split("; ")
                
                for i in range(len(auwaf_list)):
                    auaf=auwaf_list[i].split("., ")
                    try:
                        fullname=auaf[1]
                    except:
                        continue
                    countries=[]
                    for name in fullname.split(", "):
                        match,rating=process.extractOne(name,country_list,scorer=fuzz.ratio)
                        if rating>90:
                            try:
                                countries.append(iso3166.countries_by_name.get(name.upper()).alpha2)
                            except:
                                countries.apppend("")
                        elif "United Kingdom" in name:
                            countries.append("GB")
                    inst.append({"name":fullname,"author":auaf[0],"countries":countries})

        return inst

    def parse_source(self,reg):
        """
        Transforms the raw register source information from Scopus in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Scopus format
        
        Returns
        -------
        source : dict
           Information of the source in the CoLav standard format
        """
        source={}

        if "Source title" in reg.keys():
            if reg["Source title"] and reg["Source title"]==reg["Source title"]:
                source["title"]=reg["Source title"]
            else:
                source["title"]=""
        else:
            source["title"]=""
        if "Abbreviated Source Title" in reg.keys():
            source["abbreviations"]=[]
            source["abbreviations"].append({"type":"unknown","value":reg["Abbreviated Source Title"]})
        source["serials"]=[]
        if "ISSN" in reg.keys():
            if reg["ISSN"] and reg["ISSN"]==reg["ISSN"]:
                source["serials"].append({"type":"unknown","value":reg["ISSN"]})
            if reg["ISBN"] and reg["ISBN"]==reg["ISBN"]:
                source["serials"].append({"type":"isbn","value":reg["ISBN"]})
            if  reg["CODEN"] and reg["CODEN"]==reg["CODEN"]:
                source["serials"].append({"type":"coden","value":reg["CODEN"]})
        source["publisher"]=""

        return source


    def parse_one(self,register):
        """
        Transforms the raw register from Scopus in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Scopus format
        
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
                self.parse_authors(register),
                self.parse_institutions(register),
                self.parse_source(register))
        
        
        #if "" in reg.keys(): 

        #REFERENCES SECTION
        references={}
        if "references_count" in reg.keys(): data["references_count"]=reg["references_count"]
        if "references" in reg.keys():
            pass
        data["references"]=references

        #CITATION SECTION    
        citations={}
        if "scholarly_citations_count" in reg.keys(): data["citations_count"]=reg["scholarly_citations_count"]
        if "scholarly_citations" in reg.keys():
            pass
        data["citations"]=citations

        return data

    def parse_many(self,registers):
        '''
        '''
        data=[]
        for reg in registers:
            try:
                data.append(self.parse_one(reg))
            except Exception as e:
                print("Could not parse register")
                print(reg)
                print(e)
        return data