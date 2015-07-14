import csv, json, re
from py2neo import Graph, Path
from alchemyapi import AlchemyAPI
import json, sys

# Create the AlchemyAPI Object
alchemyapi = AlchemyAPI()

art_keywords = {}
art_entities = {}

graph = Graph("http://neo4j:root@localhost:7474/db/data/")

article_query =  """MATCH (n:Article{processed:'no'}) 
					Return distinct n.url as url"""

result = graph.cypher.execute(article_query)

for arti in result:

	art = arti['url']

	article_node = """ MATCH (article:Article{url:'"""+art+"""'})
	SET article.processed = 'yes'
	Return article;
	"""
	article = graph.cypher.execute(article_node)

	if art not in art_keywords.keys():
		art_keywords[art] = []
		response = alchemyapi.keywords('url', art, {'sentiment': 1})
		if response['status'] == 'OK':
			for keyword in response['keywords']:
				# print('text: ', keyword['text'].encode('utf-8'))
				key = str(keyword['text'].encode('utf-8')).replace("'","")
				art_keywords[art].append(key)

				keyword_node = """ MERGE (keyword:Keywords{text:'"""+key+"""'})
				Return keyword;
				"""
				at_keywords = graph.cypher.execute(keyword_node)

				graph.create(Path(article.one,"has_keyword",at_keywords.one))


	if art not in art_entities.keys():
		art_entities[art] = []
		response = alchemyapi.entities('url', art, {'sentiment': 1})
		if response['status'] == 'OK':
			for entities in response['entities']:
				# print('text: ', entities['text'].encode('utf-8'))
				key = str(entities['text'].encode('utf-8')).replace("'","")
				art_entities[art].append(key)

				entities_node = """ MERGE (entities:Entities{text:'"""+key+"""'})
				Return entities;
				"""
				at_entities = graph.cypher.execute(entities_node)

				graph.create(Path(article.one,"has_entity",at_entities.one))
