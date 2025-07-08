import polars as pl
import cx_Oracle as orcx
from minio import Minio
import io

def main():
	# Initialize Oracle client
	orcx.init_oracle_client(lib_dir='D:/instantclient_21.0')

	# Define the connection string
	conn = 'oracle://jinair_read:hDtxZgzrfgXCtPv2QmEH@lj.db.rep.mrva.io:1521/ORCL'

	query = """SELECT dur.SRCI, dur.AGTN, TO_CHAR(dur.DAIS, 'YYYY-MM-DD') DAIS, dur.AGTN_NAME, dur.PXNM, dur.TRNC, dur.CPNR, dur.TDNR, 
		dur.TACN, TO_CHAR(dur.FTDA, 'YYYY-MM-DD') FTDA, dur.FTNR, dur.ORAC, dur.DSTC, dur.RBKD, dur.TPAX, dur.NPAX, dur.EMIS_CUTP CUTP, 
		dur.CPVL-dur.CORT_COAM-dur.CORT_SPAM-dur.CPCM_OTHR NET_FARE, dur.DOM_INT, dur.ROUTING ROUTE
	FROM EDGAR_FIN.DWH_UPLIFT_RPT dur 
	WHERE dur.SMOD = 'NEW' AND dur.DOM_INT = 'INT' AND dur.TDNR = '2834578058' 
		AND dur.SRCI IN ('AGTWKR', 'BSP-KR') AND dur.TACN = '718' 
		AND dur.RBKD NOT IN ('G', 'G1', 'G2', 'G3', 'I')
		AND dur.MNTH = '2025-03-01'"""

	df = pl.read_database_uri(query=query, uri=conn, engine='connectorx') 

	print(df)

def test():
	bucket_name = "datalake"

	df = read_parquet_from_minio(bucket_name, object_name = "vi/edgar_uplift_int_2025-05.parquet")
	agent = read_csv_from_minio(bucket_name, object_name = 'vi/agent.csv')

	df1 = df.filter(
		pl.col('SectorDomInt') == 'INT',
		pl.col('Issuing Airline') == 718,
		pl.col('Source').is_in(['AGTWKR', 'BSP-KR']),
		~pl.col('RBD').is_in(['G', 'G1', 'G2', 'G3', 'I'])
	).group_by(
		pl.col('Original Sales Agent').alias('AGTN'),
		pl.col('Original Agent Name').alias('AGTN_NAME'),
	).agg(
		pl.len().alias('PAX_COUNT'),
		pl.col('DiscountedFare').sum().alias('NET_FARE'),
	).join(
		agent, how='left', on='AGTN'
	)

	df1 = df1.with_columns(
		MAIN_AGTN = pl.when(pl.col('MAIN_AGTN').is_null())
			.then(pl.col('AGTN'))
			.otherwise(pl.col('MAIN_AGTN')),
		MAIN_AGTN_NAME = pl.when(pl.col('MAIN_AGTN_NAME').is_null())
			.then(pl.col('AGTN_NAME'))
			.otherwise(pl.col('MAIN_AGTN_NAME')),
	)
	
	df2 = df1.group_by(
		pl.col('MAIN_AGTN'),
		pl.col('MAIN_AGTN_NAME'),
	).agg(
		pl.col('PAX_COUNT').sum(),
		pl.col('NET_FARE').sum(),
	).filter(
		pl.col('NET_FARE') > 300000000,
	).sort(
		'NET_FARE', descending=True
	)
	print(df2)
	

def read_parquet_from_minio(bucket_name, object_name):
	"""
	Read a Parquet file from MinIO and return a Polars DataFrame.
	"""
	client = Minio(
	"10.90.65.61:9000",
	access_key="X6I698S1TZ4N791O9PK2",
	secret_key="nSd6SEEMxVrI5IdD06itUMGt+44StxTiz5i7uVpa",
	secure=False,  # Set to True if using HTTPS
)
	response = client.get_object(bucket_name, object_name)

	# Read the file content into a Polars DataFrame
	data = io.BytesIO(response.read())
	df = pl.read_parquet(data)
	return df

def read_csv_from_minio(bucket_name, object_name):
	"""
	Read a Parquet file from MinIO and return a Polars DataFrame.
	"""
	client = Minio(
	"10.90.65.61:9000",
	access_key="X6I698S1TZ4N791O9PK2",
	secret_key="nSd6SEEMxVrI5IdD06itUMGt+44StxTiz5i7uVpa",
	secure=False,  # Set to True if using HTTPS
)
	response = client.get_object(bucket_name, object_name)
	df = pl.read_csv(response)
	return df

if __name__ == "__main__":
	test()	