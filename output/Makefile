all:
	dot database.gv -T png > database.png
	dot database.gv -T svg > database.svg

publish:
	s3cmd put database.tsv s3://s3.macbre.net/data_flow/database.tsv --acl-public --add-header=Access-Control-Allow-Origin:*
