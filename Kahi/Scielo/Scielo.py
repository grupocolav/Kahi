from time import time
from datetime import datetime as dt
import iso3166
import iso639
import json
import Levenshtein

# TODO:
# * Check how the email, orcidid and researcherid in the author information
#   could be matched to the corresponding author in the list
# * What to do with the inconsistent lastnames?
# * How to find an institution with the given data?

class Scielo():
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
        #Depending on the type of publication
        #this should fill fields with different information
        #For example if it is an article a book or a conference
        #check the legend because PT and DT are confusing
        if "DT" in register.keys():
            if register["DT"] and register["DT"]==register["DT"]:
                data["publication_type"]=register["DT"].rstrip().lower()
            else:
                data["publication_type"]=""
        else:
            data["publication_type"]=""
        if "PT" in register.keys():
            if register["PT"].rstrip()=="J":
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
                else:
                    data["author_count"]=""
            elif register["PT"].rstrip()=="B":
                data["publication_type"]="book"
                if "BA" in register.keys():
                    data["author_count"]=len(register["BA"].rstrip().split("\n"))
                else:
                    data["author_count"]=""
            elif register["PT"].rstrip()=="S":
                data["publication_type"]="series"
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
                else:
                    data["author_count"]=""
            elif register["PT"].rstrip()=="P":
                data["publication_type"]="patent"
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
                else:
                    data["author_count"]=""
            else:
                data["publication_type"]=""
                if "AU" in register.keys():
                    data["author_count"]=len(register["AU"].rstrip().split("\n"))
                else:
                    data["author_count"]=""
        else:
            data["publication_type"]=""
            if "AU" in register.keys():
                data["author_count"]=len(register["AU"].rstrip().split("\n"))
            else:
                data["author_count"]=""
        
            
        if "TI" in register.keys():
            if register["TI"] and register["TI"]==register["TI"]:
                data["title"]=register["TI"].rstrip()
            else:
                data["title"]=""
        if "AB" in register.keys():
            if register["AB"] and register["AB"]==register["AB"]:
                data["abstract"]=register["AB"].rstrip()
            else:
                data["abstract"]=""
        else:
            data["abstract"]=""
        if "BP" in register.keys():
            if register["BP"] and register["BP"]==register["BP"]:
                try:
                    data["start_page"]=int(register["BP"].rstrip())
                except:
                    data["start_page"]=""
            else:
                data["start_page"]=""
        else:
            data["start_page"]=""
        if "EP" in register.keys():
            if register["EP"] and register["EP"]==register["EP"]:
                try:
                    data["end_page"]=int(register["EP"].rstrip())
                except:
                    data["end_page"]=""
            else:
                data["end_page"]=""
        else:
            data["end_page"]=""
        if "VL" in register.keys():
            if register["VL"] and register["VL"]==register["VL"]:
                try:
                    data["volume"]=int(register["VL"].rstrip())
                except:
                    data["volume"]=""
            else:
                data["volume"]=""
        else:
            data["volume"]=""
        if "IS" in register.keys():
            if register["IS"] and register["IS"]==register["IS"]:
                try:
                    data["issue"]=int(register["IS"].rstrip())
                except:
                    data["issue"]=""
            else:
                data["issue"]=""
        else:
            data["issue"]=""
        if "PY" in register.keys():
            if register["PY"] and register["PY"]==register["PY"]:
                data["year_published"]=int(register["PY"].rstrip())
            else:
                data["year_published"]=""
        else:
            data["year_published"]=""
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
            else:
                data["languages"]=""
        else:
            data["languages"]=""
        
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
                ext={"source":"scielo","id":register["UT"].rstrip().split(":")[1]}
                data["external_ids"].append(ext)

            
        data["urls"]=[]

        #REFERENCES SECTION
        if "NR" in register.keys():
            if register["NR"] and register["NR"]==register["NR"]:
                try:
                    data["references_count"]=int(register["NR"].rstrip())
                except:
                    data["references_count"]=""
            else:
                data["references_count"]=""
        else:
            data["references_count"]=""

        #CITATIONS SECTION
        if "Z9" in register.keys():
            if register["Z9"] and register["Z9"]==register["Z9"]:
                try:
                   data["citations_count"]=int(register["Z9"].rstrip())
                except:
                    data["citations_count"]=""
            else:
                data["citations_count"]=""
        else:
            data["citations_count"]=""

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
        corresponding_last_name=""
        orcid_list=[]
        researchid_list=[]
        if "RI" in register.keys():
            if register["RI"] and register["RI"]==register["RI"]:
                researchid_list=register["RI"].rstrip().split("; ")
        if "OI" in register.keys():
            if register["OI"] and register["OI"]==register["OI"]:
                orcid_list=register["OI"].rstrip().replace("; ",";").split(";")
        if "AF" in register.keys():
            author_list=register["AF"].rstrip().split("\n")
        elif "AU" in register.keys():
            author_list=register["AU"].rstrip().lower().split("\n")
        if "RP" in register.keys():
            if register["RP"]:
                corresponding_last_name=register["RP"].split(",")[0]
        for au in author_list:
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
            print(names.split(" "))
            initials="".join([i[0].upper() for i in names.split(" ")])
            entry={
                'full_name':names+" "+last_names,
                'first_names':names,
                'last_names':last_names,
                'initials':initials
            }
            #Checking if there is an external id
            entry_ext=[]
            for res in researchid_list:
                try:
                    name,rid=res.split("/")
                except:
                    continue
                if Levenshtein.ratio(name,last_names+", "+names)>0.8:
                    entry_ext.append({"source":"researchid","value":rid})
                    break
            for res in orcid_list:
                try:
                    name,oid=res.split("/")
                except:
                    continue
                if Levenshtein.ratio(name,last_names+", "+names)>0.8:
                    entry_ext.append({"source":"orcid","value":oid})
                    break
            entry["external_ids"]=entry_ext
            #Checking if is corresponding author
            if corresponding_last_name:
                if corresponding_last_name in last_names:
                    entry["correspondig"]=True
                    if "EM" in register.keys():
                        if register["EM"] and register["EM"]==register["EM"]:
                            entry["corresponding_email"]=register["EM"].rstrip()
                        else:
                            entry["corresponding_email"]=""
                    else:
                        entry["corresponding_email"]=""
                else:
                    entry["correspondig"]=False
                    entry["corresponding_email"]=""
            authors.append(entry)
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
            for auwaf in register["C1"].rstrip().split("\n"):
                aulen=len(auwaf.split(";"))
                if aulen==1:
                    aff=auwaf
                else:
                    aff=auwaf.split("] ")[1]
                name=",".join(aff.split(",")[:-1])
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
                        print("could not parse: ",aff.split(", ")[-1].replace(".","").upper())
                        country=""
                for i in range(aulen):
                    inst.append({"name":name,"country":country}) ##LAST PART OF aff HAS THE COUNTRY
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
        source["serials"]=[]
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
        source["abbreviations"]=[]
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
                self.parse_authors(register),
                self.parse_institutions(register),
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