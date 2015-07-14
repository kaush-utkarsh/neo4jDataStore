import csv, json, re
from py2neo import Graph, Path
import json, sys

graph = Graph("http://neo4j:root@localhost:7474/db/data/")

csvfile = open('Web_Page_Activity.csv')

reader = csv.DictReader(csvfile)
articles = {}
for row in reader:
	if row['Name']!='':	
		name = (str(row['Name']).replace("'",""))
		art = 'https://www.brighttalk.com'+row['Entry Page']

		article_check_query =  """MATCH (n:Article{url:'"""+art+"""'})
								Return count(n) as count"""

		res = graph.cypher.execute(article_check_query)

		if str(res[0]['count']) == '0':

			individual_node = """ MERGE (idividual:Individuals{name:'"""+name+"""'})
			Return idividual;
			"""
			individual = graph.cypher.execute(individual_node)

			article_node = """ MERGE (article:Article{url:'"""+art+"""',processed:'no'})
			Return article;
			"""
			article = graph.cypher.execute(article_node)

			graph.create(Path(individual.one,"read",article.one))

