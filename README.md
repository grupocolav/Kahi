# kahi
ETL to translate raw database information into the standard colav format


TODO:
* Scopus affiliations does not parse an entry with only a author name or the affiliation
* Universidad del altlántico se confunde con Universidade Atlântica en ROR (DOI: 10.1007/s40314-015-0289-1)
* Fix the detection of country "Reunion" in Scopus (DOI: 10.1112/s0024609300217037)
* DOI: "10.1002/1521-3951(200007)220:1<351::aid-pssb351>3.3.co;2-w" se pierde pues sólo existe en wos y no se pueden relacionar los autores con las afiliaciones
