from Kahi import Kahi

etl=Kahi.Kahi(colav_db="antioquia",n_jobs=8,verbose=1)


print('------------------------')
print('Starting Kahi ETL documents extraction')
doi_list=["10.4034/pboci.2016.161.03",
        "10.1016/j.mmcr.2016.06.001",
        "10.1016/j.jalz.2018.12.010",
        "10.1007/s00405-014-2893-x",
        "10.1371/journal.pone.0147135",
        "10.7550/rmb.37002",
        "10.1590/s0037-86822008000100002",
        "10.1590/s1020-49892003000900003",
        "10.1016/j.infect.2014.03.001",
        "10.1016/j.aprim.2013.11.007"]
etl.extract_doi(doi_list)
print('------------------------')
print('Starting Kahi ETL documents transformation')
etl.transform()
print('------------------------')
print('Starting Kahi ETL documents linking')
etl.link()
print('------------------------')
print('Starting Kahi ETL documents loading')
etl.load()
print('------------------------')
print('Finished ETL process')

