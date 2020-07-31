from Kahi import Kahi

etl=Kahi.Kahi(verbose=4)

print('------------------------')
print('Starting Kahi ETL lens dois process')
etl.update(num_jobs=72,lens=1)
print('------------------------')
print('Starting Kahi ETL wos dois process')
etl.update(num_jobs=72,lens=0,wos=1)
print('------------------------')
print('Starting Kahi ETL scielo dois process')
etl.update(num_jobs=72,lens=0,wos=0,scielo=1)
print('------------------------')
print('Starting Kahi ETL scopus dois process')
etl.update(num_jobs=72,lens=0,wos=0,scielo=0,scopus=1)
print('------------------------')

