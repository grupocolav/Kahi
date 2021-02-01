from time import time
from datetime import datetime as dt
import iso3166
import iso639
import json
from fuzzywuzzy import fuzz
from langid import classify

# TODO:
# * Check how the email, orcidid and researcherid in the author information
#   could be matched to the corresponding author in the list
# * What to do with the inconsistent lastnames?
# * How to find an institution with the given data?

class WebOfScience():
    def __init__(self,legendfile=""):
        """
        """
        if legendfile:
            with open(legendfile) as f:
                self._legend=json.load(f)

    def parse_document(self, register):
        """
        Transforms the raw register document information from web of science in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in web of science format
        
        Returns
        -------
        document : dict
            Information of the document in the CoLav standard format
        """
        data={}
        data["updated"]=int(time())
        data["source_checked"]=[{"source":"wos","ts":int(time())}]
        data["publication_type"]=""
        data["titles"]=[]
        data["subtitle"]=""
        data["abstract"]=""
        data["abstract_idx"]=""
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
        
        #Depending on the type of publication
        #this should fill fields with different information
        #For example if it is an article a book or a conference
        #check the legend because PT and DT are confusing
        if "DT" in register.keys():
            if register["DT"] and register["DT"]==register["DT"]:
                data["publication_type"]=register["DT"].rstrip().lower()
        if "PT" in register.keys():
            if register["PT"].rstrip()=="J":
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
            elif register["PT"].rstrip()=="B":
                data["publication_type"]="book"
                if "BA" in register.keys():
                    data["author_count"]=len(register["BA"].rstrip().split("\n"))
            elif register["PT"].rstrip()=="S":
                data["publication_type"]="series"
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
            elif register["PT"].rstrip()=="P":
                data["publication_type"]="patent"
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
            else:
                data["publication_type"]=""
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
        else:
            data["publication_type"]=""
            if "AU" in register.keys():
                data["author_count"]=len(register["AU"].rstrip().split("\n"))
            
        if "TI" in register.keys():
            if register["TI"] and register["TI"]==register["TI"]:
                title=register["TI"].strip()
                lang=classify(title)
                data["titles"].append({"title":title,"lang":lang[0],"title_idx":title.lower()})
        if "AB" in register.keys():
            if register["AB"] and register["AB"]==register["AB"]:
                data["abstract"]=register["AB"].rstrip()
        if "BP" in register.keys():
            if register["BP"] and register["BP"]==register["BP"]:
                try:
                    data["start_page"]=int(register["BP"])
                except:
                    print("Could not transform start page to int")
        if "EP" in register.keys():
            if register["EP"] and register["EP"]==register["EP"]:
                try:
                    data["end_page"]=int(register["EP"])
                except:
                    print("Could not transform end page to int")
        if "VL" in register.keys():
            if register["VL"] and register["VL"]==register["VL"]:
                try:
                    data["volume"]=int(register["VL"].rstrip())
                except:
                    print("Could not transform volume to int")
        if "IS" in register.keys():
            if register["IS"] and register["IS"]==register["IS"]:
                try:
                    data["issue"]=int(register["IS"].rstrip())
                except:
                    print("Could not transform issue to int")
        if "PY" in register.keys():
            if register["PY"] and register["PY"]==register["PY"]:
                data["year_published"]=int(register["PY"].rstrip())
        if "LA" in register.keys():
            if register["LA"] and register["LA"]==register["LA"]:
                langs=register["LA"].rstrip().split("\n")
                if len(langs)==1:
                    if register["LA"].rstrip()=="Unspecified":
                        data["languages"]=""
                    else:
                        data["languages"]=[iso639.languages.inverted.get(register["LA"].rstrip()).part1]
                else:
                    data["languages"]=[]
                    for lang in langs:
                        if lang=="Unspecified":
                            data["languages"].append("")
                        else:
                            data["languages"].append(iso639.languages.inverted.get(lang).part1)
        
        #external_ids
        data["external_ids"]=[]
        if "DI" in register.keys():
            if register["DI"]:
                ext={"source":"doi","id":register["DI"]}
                data["external_ids"].append(ext)
        if "D2" in register.keys():
            if register["D2"]:
                ext={"source":"book_doi","id":register["D2"]}
                data["external_ids"].append(ext)
        if "UT" in register.keys():
            if register["UT"]:
                ext={"source":"wos","id":register["UT"].rstrip().split(":")[1]}
                data["external_ids"].append(ext)

        #REFERENCES SECTION
        if "NR" in register.keys():
            if register["NR"] and register["NR"]==register["NR"]:
                try:
                    data["references_count"]=int(register["NR"].rstrip())
                except:
                    print("Could not transform references count to int")

        #CITATIONS SECTION
        if "Z9" in register.keys():
            if register["Z9"] and register["Z9"]==register["Z9"]:
                try:
                   data["citations_count"]=int(register["Z9"].rstrip())
                except:
                    print("Could not transform citations count to int")

        return data

    
    def parse_authors(self,register):
        """
        Transforms the raw register author information from web of science in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in web of science format
        
        Returns
        -------
        authors : list
            Information of the authors in the CoLav standard format
        """
        authors=[]
        if "PT" in register.keys():
            #if register["PT"].rstrip()=="J":
            corresponding_last_name=""
            orcid_list=[]
            researchid_list=[]
            if "RI" in register.keys():
                if register["RI"] and register["RI"]==register["RI"]:
                    ri=register["RI"]
                    if ri[-1]=="\n":
                        ri=ri[:-1]
                    researchid_list=ri.rstrip().replace("; ",";").split(";")
            if "OI" in register.keys():
                if register["OI"] and register["OI"]==register["OI"]:
                    oi=register["OI"]
                    if oi[-1]=="\n":
                        oi=oi[:-1]
                    orcid_list=oi.rstrip().replace("; ",";").split(";")
            if "AF" in register.keys():
                author_list=register["AF"].rstrip().split("\n")
                if "RP" in register.keys():
                    if register["RP"]:
                        corresponding_last_name=register["RP"].split(",")[0]
                for au in author_list:
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
                    raw_name=au.split(", ")
                    if len(raw_name)==1:
                        names=raw_name[0].capitalize()
                        last_names=""
                    elif len(raw_name)>2:
                        names=" ".join(raw_name[:-1]).rstrip().capitalize()
                        last_names=raw_name[-1].capitalize()
                    else:
                        names=raw_name[1].capitalize()
                        last_names=raw_name[0].capitalize()

                    entry["full_name"]=names+" "+last_names
                    entry["first_names"]=names
                    entry["last_names"]=last_names
                    entry["initials"]="".join([i[0].upper() for i in names.split(" ")])
                    #Checking if there is an external id
                    entry_ext=[]
                    for res in researchid_list:
                        if not res:
                            continue
                        try:
                            name,rid=res.split("/")[-2:]
                        except Exception as e:
                            print("Could not split name and id in researchid field on ",register["doi_idx"])
                            print(e)
                        ratio=fuzz.partial_ratio(name,last_names+", "+names)
                        if ratio>90:
                            entry_ext.append({"source":"researchid","value":rid})
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(name,last_names+", "+names)
                            if ratio>90:
                                entry_ext.append({"source":"researchid","value":rid})
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(name,last_names+", "+names)
                                if ratio>95:
                                    entry_ext.append({"source":"researchid","value":rid})
                                    break
                    for res in orcid_list:
                        if not res:
                            continue
                        try:
                            name,oid=res.split("/")[-2:]
                        except Exception as e:
                            print("Could not split name and id in orcid field on ",register["doi_idx"])
                            print(e)
                        ratio=fuzz.partial_ratio(name,last_names+", "+names)
                        if ratio>90:
                            entry_ext.append({"source":"orcid","value":oid})
                            break
                        elif ratio>50:
                            ratio=fuzz.token_set_ratio(name,last_names+", "+names)
                            if ratio>90:
                                entry_ext.append({"source":"orcid","value":oid})
                                break
                            elif ratio>50:
                                ratio=fuzz.partial_token_set_ratio(name,last_names+", "+names)
                                if ratio>95:
                                    entry_ext.append({"source":"orcid","value":oid})
                                    break
                    entry["external_ids"]=entry_ext
                    #Checking if is corresponding author
                    if corresponding_last_name:
                        if corresponding_last_name in last_names:
                            entry["corresponding"]=True
                            if "EM" in register.keys():
                                if register["EM"] and register["EM"]==register["EM"]:
                                    entry["corresponding_email"]=register["EM"].rstrip()
                    authors.append(entry)
                if len(authors)==1:
                    authors[0]["corresponding"]=True
        return authors

    def parse_authors_institutions(self,register):
        authors=[]
        if "C1" in register.keys():
            if register["C1"]:

                corresponding_last_name=""
                orcid_list=[]
                researchid_list=[]
                if "RI" in register.keys():
                    if register["RI"] and register["RI"]==register["RI"]:
                        researchid_list=register["RI"].rstrip().replace("; ",";").split(";")
                if "OI" in register.keys():
                    if register["OI"] and register["OI"]==register["OI"]:
                        orcid_list=register["OI"].rstrip().replace("; ",";").split(";")
                
                institutions=self.parse_institutions(register)
                raw_authors=self.parse_authors(register)
                for author in raw_authors:
                    author["affiliations"]=[]
                    for institution in institutions:
                        if institution["author"]=="":
                            continue
                        if fuzz.token_set_ratio(author["full_name"],institution["author"])>=80:
                            inst=institution.copy()
                            del(inst["author"])
                            author["affiliations"].append(inst)
                            break
                    authors.append(author)
                for institution in institutions: #searching empty institutions and append a false author
                    if institution["author"]=="":
                        entry={
                            "full_name":"missing",
                            "first_names":"",
                            "last_names":"",
                            "initials":"",
                            "external_ids":[],
                            "aliases":[],
                            "corresponding":"",
                            "corresponding_address":"",
                            "corresponding_email":"",
                            "updated":666,
                            "affiliations":[institution]
                        }
                        authors.append(entry)
            else:
                authors = self.parse_authors(register)
                return authors
        else:
            authors = self.parse_authors(register)
            return authors

        

        return authors

    def parse_institutions(self,register):
        """
        Transforms the raw register institution informatio from web of science in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in web of science format
        
        Returns
        -------
        institutions : list
            Information of the institutions in the CoLav standard format
        """
        inst=[]
        #if "" in register.keys(): inst[""]=register[""]
        if "C1" in register.keys():
            #print(register["C1"].rstrip().split("\n"))
            C1=register["C1"].strip().replace(".","")
            for auwaf in C1.split("\n"):
                aulen=len(auwaf.split(";"))
                if aulen==1:
                    auaff=auwaf.split("] ")
                    if len(auaff)==1:
                        aff=auwaf
                        authors=[""]
                        if "AF" in register.keys():
                            if len(register["AF"].rstrip().split("\n"))==1:
                                authors=register["AF"].rstrip()
                    else:
                        aff=auaff[1]
                        authors=[auaff[0][1:]]
                else:
                    aff=auwaf.split("] ")[1]
                    authors=auwaf.split("] ")[0][1:].split("; ")
                try:
                    name="".join(aff.split(", ")[0])
                except:
                    name=""
                if aff.split(", ")[-1].replace(".","").upper()=="ENGLAND":
                    country="GB"
                elif aff.split(", ")[-1].replace(".","").upper()=="CZECH REPUBLIC":
                    country="CZ"
                elif aff.split(", ")[-1].replace(".","").upper()=="VENEZUELA":
                    country="VE"
                elif aff.split(", ")[-1].replace(".","").upper()=="VIETNAM":
                    country="VN"
                elif aff.split(", ")[-1].replace(".","").upper()=="RUSSIA":
                    country="RU"
                elif aff.split(", ")[-1].replace(".","").upper()=="PEOPLES R CHINA":
                    country="CN"
                elif aff.split(", ")[-1].replace(".","").upper()=="SCOTLAND":
                    country="GB"
                elif aff.split(", ")[-1].replace(".","").upper()=="IRAN":
                    country="IR"
                elif aff.split(", ")[-1].replace(".","").upper()=="SOUTH KOREA":
                    country="KR"
                elif aff.split(", ")[-1].replace(".","").upper()=="U ARAB EMIRATES":
                    country="AE"
                elif aff.split(", ")[-1].replace(".","").upper()=="DEM REP CONGO":
                    country="CD"
                elif aff.split(", ")[-1].replace(".","").upper()=="TANZANIA":
                    country="TZ"
                elif aff.split(", ")[-1].replace(".","").upper()=="TAIWAN":
                    country="TW"
                elif aff.split(", ")[-1].replace(".","").upper()=="WALES":
                    country="GB"
                elif aff.split(", ")[-1].replace(".","").upper()=="MICRONESIA":
                    country="FM"
                elif aff.split(", ")[-1].replace(".","").upper()[-3:]=="USA":
                    country="US"
                else:
                    try:
                        country=iso3166.countries_by_name.get(aff.split(", ")[-1].replace(".","").upper()).alpha2
                    except:
                        #print("could not parse: ",aff.split(", ")[-1].replace(".","").upper())
                        country=""
                for i in range(len(authors)):
                    author=authors[i] if authors else ""
                    entry_aff={"name":name,
                                "aliases":[],
                                "abbreviations":[],
                                "external_ids":[],
                                "types":[],
                                "relationships":[],
                                "external_urls":[],
                                "author":author,
                                "addresses":[{"country":country}]
                    }
                    inst.append(entry_aff) ##LAST PART OF aff HAS THE COUNTRY
        return inst

    def parse_source(self,register):
        """
        Transforms the raw register source information from web of science in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in web of science format
        
        Returns
        -------
        source : dict
           Information of the source in the CoLav standard format
        """
        source={}
        source["title"]=""
        source["serials"]=[]
        source["institution"]=""
        source["institution_id"]=""
        source["abbreviations"]=[]
        source["publisher"]=""
        source["country"]=""
        source["submission_charges"]=""
        source["submission_currency"]=""
        source["apc_charges"]=""
        source["apc_currency"]=""
        source["subjects"]={}

        if "SO" in register.keys():
            if register["SO"]:
                source["title"]=register["SO"].rstrip()
        if "PT" in register.keys():
            if register["PT"]:
                if register["PT"].rstrip()=="J": source["type"]="journal"
                if register["PT"].rstrip()=="B": source["type"]="book"
                if register["PT"].rstrip()=="S": source["type"]="series"
                if register["PT"].rstrip()=="P": source["type"]="patent"
        if "PU" in register.keys():
            if register["PU"]:
                source["publisher"]=register["PU"].rstrip()

        if "SN" in register.keys():
            if register["SN"]:
                entry={"type":"pissn","value":register["SN"].rstrip().replace("-","")}
                source["serials"].append(entry)
        if "EI" in register.keys():
            if register["EI"]:
                entry={"type":"eissn","value":register["EI"].rstrip().replace("-","")}
                source["serials"].append(entry)
        if "BN" in register.keys():
            if register["BN"]:
                entry={"type":"isbn","value":register["BN"].rstrip().replace("-","")}
                source["serials"].append(entry)

        if "J9" in register.keys():
            if register["J9"]:
                entry={"type":"char","value":register["J9"].rstrip()}
                source["abbreviations"].append(entry)
        if "JI" in register.keys():
            if register["JI"]:
                entry={"type":"iso","value":register["JI"].rstrip()}
                source["abbreviations"].append(entry)
        #if "" in register.keys(): source[""]=register[""]
        #if "" in register.keys(): source[""]=register[""]
        #if "" in register.keys(): source[""]=register[""]
        
        return source
        
    def parse_one(self,register):
        """
        Transforms the raw register from Web Of Science in the CoLav standard.

        Parameters
        ----------
        register : dict
           Register in Web Of Science format
        
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
                self.parse_authors_institutions(register),
                #self.parse_institutions(register),
                self.parse_source(register))

                        
        

    def parse_many(self,registers):
        """
        Transforms a list raw register from Web Of Science in the CoLav standard.

        Parameters
        ----------
        registers : list
           Register in Web Of Science format
        
        Returns????????????????????????????????
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
        data=[]
        for reg in registers:
            try:
                data.append(self.parse_one(reg))
            except Exception as e:
                print("Could not parse register")
                print(reg)
                print(e)
        return data
