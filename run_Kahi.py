from Kahi import Kahi

#etl=Kahi.Kahi(colav_db="antioquia",n_jobs=8,verbose=1)
etl=Kahi.Kahi(colav_db="antioquia",ror_url="http://172.19.31.9:9292/organizations?affiliation=",n_jobs=8,verbose=1)

print('------------------------')
print('Starting Kahi ETL documents extraction')
doi_list=['10.1186/1745-6215-7-19',
 '10.1016/j.rcpeng.2017.12.006',
 '10.1645/ge-1977.1',
 '10.1016/s1474-4422(12)70228-4',
 '10.1093/jnci/35.1.15',
 '10.1680/jgein.18.00004',
 '10.18566/rfdcp.v47n127.a05',
 '10.1016/j.aml.2008.02.014',
 '10.3390/en11020441',
 '10.1109/sifae.2012.6478890',
 '10.1088/0953-8984/23/2/025301',
 '10.1016/j.ssi.2019.115199',
 '10.3389/fnagi.2013.00080',
 '10.1016/j.surfcoat.2017.07.086',
 '10.1016/j.rccar.2014.06.005',
 '10.1155/2018/5379047',
 '10.1590/1413-81232020253.01322018',
 '10.1007/978-3-030-23816-2_46',
 '10.1109/stsiva.2015.7330399',
 '10.21932/epistemus.5.3751.1',
 '10.22379/2422402276',
 '10.3390/en10101449',
 '10.1056/nejmicm1511213',
 '10.21840/siic/156482',
 '10.15446/rev.fac.cienc.v8n1.74526',
 '10.1016/j.hivar.2014.02.007',
 '10.22463/17949831.1606',
 '10.1016/j.jclinepi.2017.05.016',
 '10.14306/renhyd.19.4.174',
 '10.30944/20117582.40',
 '10.1364/hilas.2012.jt2a.16',
 '10.17533/udea.iatreia.v31n4a01',
 '10.16925/od.v12i22.1201',
 '10.21615/cesmedicina.32.1.2',
 '10.22516/25007440.18',
 '10.1016/j.lfs.2014.12.011',
 '10.4317/medoral.21292',
 '10.4088/jcp.08m04673yel',
 '10.1603/an12111',
 '10.11646/zootaxa.4122.1.70',
 '10.17533/udea.ikala.v19n2a02',
 '10.1088/0143-0807/24/3/304',
 '10.22354/in.v23i2.778',
 '10.1016/j.jsxm.2016.11.200',
 '10.14306/renhyd.20.4.231',
 '10.1007/s00784-007-0147-7',
 '10.15446/rev.colomb.biote.v21n1.80248',
 '10.17151/rasv.2019.21.1.9',
 '10.15517/revedu.v43i2.30564',
 '10.18273/revion.v29n1-2016005']

etl.extract_doi(doi_list)
print('------------------------')
print('Starting Kahi ETL documents transformation')
etl.parallel_transform()
print('------------------------')
print('Starting Kahi ETL documents linking')
etl.parallel_link()
print('------------------------')
print('Starting Kahi ETL documents loading')
print(etl.transformed[0])
#etl.load()
print('------------------------')
print('Finished ETL process')

