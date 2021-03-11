from Kahi import Kahi

etl=Kahi.Kahi(colav_db="antioquia",n_jobs=5,verbose=1)
#etl=Kahi.Kahi(dbserver_url="172.19.31.5",colav_db="antioquia",ror_url="http://172.19.31.9:9292/organizations?affiliation=",n_jobs=72,verbose=1)

print('------------------------')
print('Starting Kahi ETL documents extraction')

#etl.extract_doi(doi_list)
#etl.extract_from_collection("oadoi_antioquia","stage","doi_idx")
#print('------------------------')
#print('Starting Kahi ETL documents transformation')
#etl.parallel_transform()
#print('------------------------')
#print('Starting Kahi ETL documents linking')
#etl.parallel_link()
#print('------------------------')
#print('Starting Kahi ETL documents loading')
#print(etl.transformed[0])
#etl.load()
#print('------------------------')
#print('Finished ETL process')
etl.parallel_all_from_collection("oadoi_antioquia","stage","doi_idx")

