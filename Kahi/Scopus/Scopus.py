from time import time
from datetime import datetime as dt
import iso3166
import iso639
from fuzzywuzzy import fuzz,process
from re import split,UNICODE
from geotext import GeoText

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
        data["source_checked"]=[{"source":"scopus","ts":int(time())}]
        data["publication_type"]=""
        data["titles"]=[]
        data["subtitle"]=""
        data["abstract"]=""
        data["abstract_idx"]=""
        data["keywords"]=[]
        data["start_page"]=""
        data["end_page"]=""
        data["volume"]=""
        data["issue"]=""
        data["date_published"]=""
        data["year_published"]=""
        data["languages"]=[]
        data["references_count"]=""
        data["references"]=[]
        data["citations_count"]=""
        data["citations"]=[]
        data["citations_link"]=""
        data["funding_details"]=""
        data["is_open_access"]=""
        data["access_status"]=""
        data["external_ids"]=[]
        data["urls"]=[]
        data["source"]=""
        data["author_count"]=""
        data["authors"]=[]


        if "Title" in reg.keys():
            data["title"]=reg["Title"]
        if "Year" in reg.keys(): data["year_published"]=reg["Year"]
        if "Volume" in reg.keys(): 
            if reg["Volume"] and reg["Volume"]==reg["Volume"]:
                data["volume"]=reg["Volume"]
        if "Issue" in reg.keys():
            if reg["Issue"] and reg["Issue"]==reg["Issue"]:
                data["issue"]=reg["Issue"]
        if "Page start" in reg.keys():
            if reg["Page start"] and reg["Page start"] == reg["Page start"]: #checking for NaN in the second criteria
                try:
                    data["start_page"]=int(reg["Page start"])
                except:
                    print("Could not transform start page in int")
        if "Page end" in reg.keys():
            if reg["Page end"] and reg["Page end"] == reg["Page end"]:
                try:
                    data["end_page"]=int(reg["Page end"])
                except:
                    print("Could not transform end page in int")

        if "Abstract" in reg.keys():
            if reg["Abstract"] and reg["Abstract"]==reg["Abstract"]:
                data["abstract"]=reg["Abstract"]
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

        for i in range(1,5):
            texts=[]
            if "Funding Text "+str(i) in reg.keys():
                if reg["Funding Text "+str(i)] and reg["Funding Text "+str(i)] == reg["Funding Text "+str(i)]:
                    texts.append(reg["Funding Text "+str(i)])
            if len(texts) != 0:
                data["funding_details"]=texts
                
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


        data["urls"]=[]
        if "Link" in reg.keys(): data["urls"].append({"source":"scopus","url":reg["Link"]})

        #REFERENCES SECTION
        if "References" in reg.keys():
            if reg["References"] and reg["References"]==reg["References"]:
                references=reg["References"].split("; ")
                data["references_count"]=len(references)

        #CITATION SECTION
        if "Cited by" in reg.keys():
            if reg["Cited by"] and reg["Cited by"]==reg["Cited by"]:
                data["citations_count"]=int(reg["Cited by"])

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
                corresponding_list=reg["Correspondence Address"].split("; ")
                if len(corresponding_list)>0: corresponding_author=corresponding_list[0]
                if len(corresponding_list)>1: corresponding_address=corresponding_list[1]
                if len(corresponding_list)>2: corresponding_email=corresponding_list[2]
        if "Authors" in reg.keys():
            if reg["Authors"] and reg["Authors"]==reg["Authors"]:
                if "Author(s) ID" in reg.keys(): ids=reg["Author(s) ID"].split(";")
                for idx,author in enumerate(reg["Authors"].split(", ")):
                    no_corresponding=True
                    entry={}
                    entry["first_names"]=""
                    entry["national_id"]=""
                    entry["last_names"]=""
                    entry["initials"]=""
                    entry["full_name"]=""
                    entry["aliases"]=[]
                    entry["affiliations"]=[]
                    entry["keywords"]=[]
                    entry["external_ids"]=[]
                    entry["corresponding"]=False
                    entry["corresponding_address"]=""
                    entry["corresponding_email"]=""
                    try:
                        entry["full_name"]=author
                        entry["last_names"]=author.split(" ")[0]
                        entry["initials"]=author.split(" ")[1].replace(".","")
                    except Exception as e:
                        print("Could not parse author name in ",reg["doi_idx"])
                        print(e)
                    #print("\n")
                    #print(author,corresponding_author)
                    #print("\n")
                    if corresponding_author:
                        #print(author,corresponding_author)
                        rate=fuzz.partial_ratio(author,corresponding_author)
                        if rate>90:
                            entry["corresponding"]=True
                            entry["corresponding_address"]=corresponding_address
                            entry["corresponding_email"]=corresponding_email.replace("email: ","")
                            no_corresponding=False
                        elif rate>50:
                            rate=fuzz.token_set_ratio(author,corresponding_author)
                            if rate>90:
                                entry["corresponding"]=True
                                entry["corresponding_address"]=corresponding_address
                                entry["corresponding_email"]=corresponding_email.replace("email: ","")
                            elif rate>50:
                                rate=fuzz.partial_token_set_ratio(author,corresponding_author)
                                if rate>90:
                                    entry["corresponding"]=True
                                    entry["corresponding_address"]=corresponding_address
                                    entry["corresponding_email"]=corresponding_email.replace("email: ","")
                    if ids:
                        try:
                            entry["external_ids"]=[{"source":"scopus","value":ids[idx]}]
                        except Exception as e:
                            print("Could not parse author scopus id in ",reg["doi_idx"])
                            print(e)
                    authors.append(entry)
                if len(authors)==1:
                    authors[0]["corresponding"]=True



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
        if "Authors with affiliations" in reg.keys():
            if reg["Authors with affiliations"] and reg["Authors with affiliations"]==reg["Authors with affiliations"]:
                auwaf_list=reg["Authors with affiliations"].split("; ")
                for i in range(len(auwaf_list)):
                    auaf=split('(^[\w\-\s\.]+,\s+[\w\s\.\-]+,\s)',auwaf_list[i],UNICODE)
                    if len(auaf)==1:
                        author=auaf[0]
                        affiliations=""
                    else:
                        author=auaf[1]
                        affiliations=auaf[-1]
                    countries=GeoText(affiliations).countries
                    for country in countries[:-1]:
                        country_affiliation_list=affiliations.split(country+', ')
                        if country=="United Kingdom":
                            country_alpha2="GB"
                        elif country.lower()=="venezuela":
                            country_alpha2='VE'
                        elif country.lower()=="united states":
                            country_alpha2='US'
                        elif country.lower()=="czech republic":
                            country_alpha2="CZ"
                        elif country.lower()=="vietnam":
                            country_alpha2="VN"
                        elif country.lower()=="russia":
                            country_alpha2="RU"
                        elif country.lower()=="peoples r china":
                            country_alpha2="CN"
                        elif country.lower()=="scotland":
                            country_alpha2="GB"
                        elif country.lower()=="iran":
                            country_alpha2="IR"
                        elif country.lower()=="south korea":
                            country_alpha2="KR"
                        elif country.lower()=="u arab emirates":
                            country_alpha2="AE"
                        elif country.lower()=="dem rep congo":
                            country_alpha2="CD"
                        elif country.lower()=="tanzania":
                            country_alpha2="TZ"
                        elif country.lower()=="taiwan":
                            country_alpha2="TW"
                        elif country.lower()=="wales":
                            country_alpha2="GB"
                        elif country.lower()=="micronesia":
                            country_alpha2="FM"
                        elif country.lower()=="reunion":
                            continue
                        else:
                            try:
                                country_alpha2=country=iso3166.countries_by_name.get(country.upper()).alpha2
                            except:
                                #print("Could not parse: ",country)
                                country_alpha2=""
                        inst.append({"name":country_affiliation_list[0]+country,"author":author,"countries":country_alpha2})
                        affiliations=country_affiliation_list[-1] #what is left
                    try:
                        #print(i,countries[-1])
                        if countries[-1].lower()=="united kingdom":
                            country_alpha2="GB"
                        elif countries[-1].lower()=="venezuela":
                            country_alpha2='VE'
                        elif countries[-1].lower()=="united states":
                            country_alpha2='US'
                        elif countries[-1].lower()=="czech republic":
                            country_alpha2="CZ"
                        elif countries[-1].lower()=="vietnam":
                            country_alpha2="VN"
                        elif countries[-1].lower()=="russia":
                            country_alpha2="RU"
                        elif countries[-1].lower()=="peoples r china":
                            country_alpha2="CN"
                        elif countries[-1].lower()=="scotland":
                            country_alpha2="GB"
                        elif countries[-1].lower()=="iran":
                            country_alpha2="IR"
                        elif countries[-1].lower()=="south korea":
                            country_alpha2="KR"
                        elif countries[-1].lower()=="u arab emirates":
                            country_alpha2="AE"
                        elif countries[-1].lower()=="dem rep congo":
                            country_alpha2="CD"
                        elif countries[-1].lower()=="tanzania":
                            country_alpha2="TZ"
                        elif countries[-1].lower()=="taiwan":
                            country_alpha2="TW"
                        elif countries[-1].lower()=="wales":
                            country_alpha2="GB"
                        elif countries[-1].lower()=="micronesia":
                            country_alpha2="FM"
                        else:
                            country_alpha2=country=iso3166.countries_by_name.get(countries[-1].upper()).alpha2
                    except:
                        #print("Could not parse country")
                        country_alpha2=""
                    inst.append({"name":affiliations,"author":author,"countries":country_alpha2})
                    """if len(auaf)==1:
                        auaf=auwaf_list[i].split(".")
                        try:
                            fullname=auaf[1]
                        except:
                            try:
                                fullname=auaf[0]
                            except:
                                continue
                    else:
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
                                countries.append("")
                        elif "United Kingdom" in name:
                            countries.append("GB")
                    inst.append({"name":fullname,"author":affiliation,"countries":countries})"""

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
        source["title"]=""
        source["serials"]=[]
        source["abbreviations"]=[]
        source["publisher"]=""
        source["country"]=""
        source["subjects"]={}

        if "Source title" in reg.keys():
            if reg["Source title"] and reg["Source title"]==reg["Source title"]:
                source["title"]=reg["Source title"]

        if "Abbreviated Source Title" in reg.keys():
            source["abbreviations"].append({"type":"unknown","value":reg["Abbreviated Source Title"]})
        
        if "ISSN" in reg.keys():
            if reg["ISSN"] and reg["ISSN"]==reg["ISSN"]:
                source["serials"].append({"type":"unknown","value":reg["ISSN"]})
            if reg["ISBN"] and reg["ISBN"]==reg["ISBN"]:
                source["serials"].append({"type":"isbn","value":reg["ISBN"]})
            if  reg["CODEN"] and reg["CODEN"]==reg["CODEN"]:
                source["serials"].append({"type":"coden","value":reg["CODEN"]})

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

