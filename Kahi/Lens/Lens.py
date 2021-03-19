from time import time
from datetime import datetime as dt
import iso3166
from langid import classify

class Lens():
    def __init__(self):
        '''
        Base class to parse Lens data into colva standard
        '''
        pass

    def parse_document(self, reg):
        """
        Transforms the raw register document information from Lens in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Lens format
        
        Returns
        -------
        document : dict
            Information of the document in the CoLav standard format
        """
        data={}
        data["updated"]=int(time())
        data["source_checked"]=[{"source":"lens","ts":int(time())}]
        data["publication_type"]=""
        data["titles"]=[]
        data["subtitle"]=""
        data["abstract"]=""
        data["bibtex"]=""
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
        data["funding_organization"]=""
        data["funding_details"]=""
        data["is_open_access"]=""
        data["open_access_status"]=""
        data["external_ids"]=[]
        data["urls"]=[]
        data["source"]=""
        data["author_count"]=""
        data["authors"]=[]


        if "publication_type" in reg.keys():
            if reg["publication_type"] and reg["publication_type"]==reg["publication_type"]:
                data["publication_type"]=reg["publication_type"].lower() #Names of types of publications?
        if "title" in reg.keys():
            title=reg["title"]
            lang=classify(title)
            data["titles"].append({"title":title,"lang":lang[0]})
        if "abstract" in reg.keys():
            if reg["abstract"] and reg["abstract"]==reg["abstract"]:
                data["abstract"]=reg["abstract"]
        if "start_page" in reg.keys():
            if reg["start_page"] and reg["start_page"]==reg["start_page"]:
                try:
                    data["start_page"]=int(reg["start_page"])
                except:
                    print("Could not transform start page in int")
        if "end_page" in reg.keys():
            if reg["end_page"] and reg["end_page"]==reg["end_page"]:
                try:
                    data["end_page"]=int(reg["end_page"])
                except:
                    print("Could not transform end page in int")
        if "volume" in reg.keys():
            if reg["volume"] and reg["volume"]==reg["volume"]:
                data["volume"]=reg["volume"]
        if "issue" in reg.keys():
            if reg["issue"] and reg["issue"]==reg["issue"]:
                data["issue"]=reg["issue"]
        if "languages" in reg.keys():
            if reg["languages"] and reg["languages"]==reg["languages"]:
                data["languages"]=reg["languages"]
        if "year_published" in reg.keys():
            if reg["year_published"] and reg["year_published"]==reg["year_published"]:
                data["year_published"]=reg["year_published"]
        if "date_published" in reg.keys():
            if "date" in reg["date_published"].keys():
                if reg["date_published"]["date"] and reg["date_published"]["date"]==reg["date_published"]["date"]:
                    try:
                        data["date_published"]=int(dt.strptime(reg["date_published"]["date"],"%Y-%m-%dT%H:%M:%S%z").timestamp())
                    except:
                        print("Something went wrong parsing the date published")

        if "authors_count" in reg.keys():
            if reg["author_count"] and reg["author_count"]==reg["author_count"]:
                data["author_count"]=reg["author_count"]
        
        if "authors" in reg.keys() and data["author_count"]=="":
            if reg["authors"] and reg["authors"]==reg["authors"]:
                data["author_count"]=len(reg["authors"])
        
        data["external_ids"]=[]
        if "lens_id" in reg.keys():
            if reg["lens_id"] and reg["lens_id"]==reg["lens_id"]:
                data["external_ids"].append({"source":"lens","id":reg["lens_id"]})

        if "external_ids" in reg.keys():
            if reg["external_ids"] and reg["external_ids"]==reg["external_ids"]:
                for ext in reg["external_ids"]:
                    if "type" in ext.keys():
                        data["external_ids"].append({"source":ext["type"],"id":ext["value"]})
                    if "source" in ext.keys():
                        data["external_ids"].append({"source":ext["source"],"id":ext["id"]})
        
        #URLS


        #REFERENCES SECTION
        if "references_count" in reg.keys():
            if reg["references_count"] and reg["references_count"]==reg["references_count"]: 
                data["references_count"]=reg["references_count"]
        references=[]
        if "references" in reg.keys():
            if reg["references"] and reg["references"]==reg["references"]:
                for ref in reg["references"]:
                    references.append(ref["lens_id"])
        if data["references_count"]=="" and len(references)!=0:
            data["references_count"]=len(references)
            
        data["references"]=references

        #CITATION SECTION    
        citations=[]
        if "scholarly_citations_count" in reg.keys(): data["citations_count"]=int(reg["scholarly_citations_count"])

        return data

    def parse_authors(self,reg):
        """
        Transforms the raw register author information from Lens in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Lens format
        
        Returns
        -------
        authors : list
            Information of the authors in the CoLav standard format
        """
        authors=[]
        if "authors" in reg.keys():
            for author in reg["authors"]:
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
                if "first_name" in author.keys():
                    if author["first_name"]==author["first_name"]:
                        entry["first_names"]=author["first_name"]

                if "last_name" in author.keys():
                    if author["last_name"]==author["last_name"]:
                        entry["last_names"]=author["last_name"]
                if "initials" in author.keys():
                    entry["initials"]=author["initials"]
                if entry["first_names"]:
                    entry["full_name"]=entry["first_names"]+" "+entry["last_names"]
                else:
                    entry["full_name"]=entry["last_names"]
                authors.append(entry)
            if len(authors)==1:
                authors[0]["corresponding"]=True

        return authors

    def parse_authors_institutions(self,reg):
        authors=[]
        if "authors" in reg.keys():
            for author in reg["authors"]:
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
                #Author info section
                if "first_name" in author.keys():
                    if author["first_name"]==author["first_name"]:
                        entry["first_names"]=author["first_name"]
                if "last_name" in author.keys():
                    if author["last_name"]==author["last_name"]:
                        entry["last_names"]=author["last_name"]
                if "initials" in author.keys():
                    entry["initials"]=author["initials"]
                if entry["first_names"]:
                    entry["full_name"]=entry["first_names"]+" "+entry["last_names"]
                else:
                    entry["full_name"]=entry["last_names"]
                #Affiliations info section
                if "affiliations" in author.keys():
                    if author["affiliations"]:
                        for aff in author["affiliations"]:
                            aff_entry={}
                            aff_entry["name"]=""
                            aff_entry["aliases"]=""
                            aff_entry["abbreviations"]=[]
                            aff_entry["addresses"]=[]
                            aff_entry["types"]=[]
                            aff_entry["external_ids"]=[]
                            aff_entry["external_urls"]=[]
                            if "name" in aff.keys():
                                aff_entry["name"]=aff["name"]
                            if "grid" in aff.keys():
                                aff_entry["external_ids"].append({"source":"grid","value":aff["grid"]["id"]})
                                if "addresses" in aff["grid"].keys():
                                    if aff["grid"]["addresses"]:
                                        if "country_code" in aff["grid"]["addresses"][0].keys():
                                            aff_entry["addresses"].append({"country":aff["grid"]["addresses"][0]["country_code"]})
                            entry["affiliations"].append(aff_entry)
                authors.append(entry)
            if len(authors)==1:
                authors[0]["corresponding"]=True

        return authors

    def parse_institutions(self,reg):
        """
        Transforms the raw register institution informatio from Lens in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Lens format
        
        Returns
        -------
        institutions : list
            Information of the institutions in the CoLav standard format
        """
        #They go in the same order as the authors
        inst=[]
        if "authors" in reg.keys():
            for author in reg["authors"]:
                if "affiliations" in author.keys():
                    if author["affiliations"]:
                        for aff in author["affiliations"]:
                            entry={}
                            entry["author"]=""
                            entry["author"]+=author["first_name"] if author["first_name"] else ""
                            entry["author"]+=" "+author["last_name"] if author["last_name"] else ""
                            if "grid" in aff.keys():
                                entry["grid_id"]=aff["grid"]["id"]
                            else:
                                entry["grid_id"]=""
                            if "name" in aff.keys():
                                entry["name"]=aff["name"]
                            else:
                                entry["name"]=""
                            inst.append(entry)
        #if "" in register.keys(): inst[""]=register[""]
        return inst

    def parse_source(self,reg):
        """
        Transforms the raw register source information from Lens in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Lens format
        
        Returns
        -------
        source : dict
           Information of the source in the CoLav standard format
        """
        source={}
        source["updated"]=""
        source["source_checked"]=[]
        source["title"]=""
        source["type"]=""
        source["publisher"]=""
        source["institution"]=""
        source["institution_id"]=""
        source["country"]=""
        source["editorial_review"]=""
        source["submission_charges"]=""
        source["submission_charges_url"]=""
        source["submmission_currency"]=""
        source["apc_charges"]=""
        source["apc_currency"]=""
        source["apc_url"]=""
        source["serials"]=[]
        source["abbreviations"]=[]
        source["subjects"]=[]
        source["keywords"]=[]
        source["languages"]=[]
        source["plagiarism_detection"]=""
        source["active"]=""
        source["publication_time"]=""

        if "source" in reg.keys():
            if "title_full" in reg["source"].keys():
                if reg["source"]["title_full"]:
                    source["title"]=reg["source"]["title_full"]

            if "publisher" in reg["source"].keys():
                if reg["source"]["publisher"] and reg["source"]["publisher"]==reg["source"]["publisher"]:
                    source["publisher"]=reg["source"]["publisher"]

            if "country" in reg["source"].keys():
                if reg["source"]["country"]:
                    if reg["source"]["country"].lower()=="united kingdom":
                        source["country"]='GB'
                    elif reg["source"]["country"].lower()=="venezuela":
                        source["country"]='VE'
                    elif reg["source"]["country"].lower()=="united states":
                        source["country"]='US'
                    elif reg["source"]["country"].lower()=="czech republic":
                        source["country"]="CZ"
                    elif reg["source"]["country"].lower()=="vietnam":
                        source["country"]="VN"
                    elif reg["source"]["country"].lower()=="russia":
                        source["country"]="RU"
                    elif reg["source"]["country"].lower()=="peoples r china":
                        source["country"]="CN"
                    elif reg["source"]["country"].lower()=="scotland":
                        source["country"]="GB"
                    elif reg["source"]["country"].lower()=="iran":
                        source["country"]="IR"
                    elif reg["source"]["country"].lower()=="south korea":
                        source["country"]="KR"
                    elif reg["source"]["country"].lower()=="u arab emirates":
                        source["country"]="AE"
                    elif reg["source"]["country"].lower()=="dem rep congo":
                        source["country"]="CD"
                    elif reg["source"]["country"].lower()=="tanzania":
                        source["country"]="TZ"
                    elif reg["source"]["country"].lower()=="taiwan":
                        source["country"]="TW"
                    elif reg["source"]["country"].lower()=="wales":
                        source["country"]="GB"
                    elif reg["source"]["country"].lower()=="micronesia":
                        source["country"]="FM"
                    elif reg["source"]["country"].lower()=="bolivia":
                        source["country"]="BO"
                    else:
                        try:
                            source["country"]=iso3166.countries_by_name.get(reg["source"]["country"].upper()).alpha2
                        except:
                            print("Could not parse: ",reg["source"]["country"].upper())
                            source["country"]=""

            serial=[]
            if "issn" in reg["source"].keys():
                if reg["source"]["issn"]:
                    for issn in reg["source"]["issn"]:
                        if issn["type"] == "print":
                            serial.append({"type":"pissn","value":issn["value"].upper()})
                        elif issn["type"] == "electronic":
                            serial.append({"type":"eissn","value":issn["value"].upper()})
                        elif issn["type"] == "unknown":
                            serial.append({"type":"unknown","value":issn["value"].upper()})
                source["serials"]=serial



        return source

    def parse_one(self,register):
        """
        Transforms the raw register from Lens in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Lens format
        
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
        if "data" in register.keys():
            reg=register["data"]
        else:
            reg=register
        return (self.parse_document(reg),
                self.parse_authors_institutions(reg),
                #self.parse_institutions(reg),
                self.parse_source(reg))

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