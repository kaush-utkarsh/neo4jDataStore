import csv, json, re
from py2neo import Graph, Path
import json, sys
from alchemyapi import AlchemyAPI


def leads():
	print "Storing Leads"
	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	csvfile = open('MarketoLeads.csv')

	reader = csv.DictReader(csvfile)

	for row in reader:
		try:
			name = (str(row['First_Name']+' '+row['Last_Name']).replace("'",""))
			email = (str(row['Email_Address']).replace("'",""))
			company = (str(row['Company_Name']).replace("'",""))
			industry = (str(row['Industry']).replace("'",""))
			community = (str(row['Community_Segment']).replace("'",""))
			sub_community = (str(row['Sub_Community']).replace("'",""))
			

			iD = row['Id']
			job = (str(row['Job_Title']).replace("'",""))

			if name!='' and email!='' and iD!='':

				individual_node = """ MERGE (idividual:Individuals{id:'"""+iD+"""',name:'"""+name+"""',email:'"""+email+"""'})
				Return idividual;
				"""
				individual = graph.cypher.execute(individual_node)


				if company!= '':
					company_node = """ MERGE (company:Company{name:'"""+company+"""'})
					Return company;
					"""
					company = graph.cypher.execute(company_node)
					graph.create(Path(individual.one,"works_at",company.one,))
				
				if community!= '':
					community_node = """ MERGE (community:Community{name:'"""+community+"""'})
					Return community;
					"""
					community = graph.cypher.execute(community_node)

					graph.create(Path(individual.one,"belongs_to_community",community.one))
				
				if sub_community!= '':
					sub_community_node = """ MERGE (sub_community:Sub_Community{name:'"""+sub_community+"""'})
					Return sub_community;
					"""
					sub_community = graph.cypher.execute(sub_community_node)

					graph.create(Path(individual.one,"belongs_to_sub_community",sub_community.one))
				

				if job != '':
					job_node = """ MERGE (job:Job_Title{name:'"""+job+"""'})
					Return job;
					"""
					job = graph.cypher.execute(job_node)

					graph.create(Path(individual.one,"works_as",job.one))


				
				if industry != '':
					industry_node = """ MERGE (industry:Industry{name:'"""+industry+"""'})
					Return industry;
					"""
					industry = graph.cypher.execute(industry_node)

					graph.create(Path(individual.one,"is_in_Industry",industry.one))
		except Exception,e:
			print e
			pass

	return "200"

def articles_func():
	
	print "storing articles"
	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	csvfile = open('Web_Page_Activity.csv')

	reader = csv.DictReader(csvfile)
	articles = {}
	for row in reader:

		try:

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


		except Exception,e:
			print e
			pass

	return "200"


def key_entity():

	print "storing keywords"
	# Create the AlchemyAPI Object
	alchemyapi = AlchemyAPI()

	art_keywords = {}
	art_entities = {}
	
	count = 0 

	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	article_query =  """MATCH (n:Article{processed:'no'}) 
						Return distinct n.url as url"""

	result = graph.cypher.execute(article_query)

	for arti in result:

		if count >= 1000:
			print "Alchemy limit exceeds"
			exit()


		art = arti['url']

		article_node = """ MATCH (article:Article{url:'"""+art+"""'})
		SET article.processed = 'yes'
		Return article;
		"""
		article = graph.cypher.execute(article_node)

		if art not in art_keywords.keys():
			art_keywords[art] = []
			response = alchemyapi.keywords('url', art, {'sentiment': 1})
			count = count + 1
			if response['status'] == 'OK':
				for keyword in response['keywords']:
					# print('text: ', keyword['text'].encode('utf-8'))
					key = str(keyword['text'].encode('utf-8')).replace("'","")
					art_keywords[art].append(key)

					rel_dict = {}

					rel_dict['relevance'] = keyword['relevance']
					rel_dict['sentiment'] = keyword['sentiment']['type']
					if 'score' in keyword['sentiment']:
						rel_dict['sentiment_score'] = keyword['sentiment']['score']

					keyword_node = """ MERGE (keyword:Keywords{text:'"""+key+"""'})
					Return keyword;
					"""
					at_keywords = graph.cypher.execute(keyword_node)
					pth = Path(article.one,("has_keyword",rel_dict),at_keywords.one)
					graph.create(pth)


		if count >= 1000:
			print "Alchemy limit exceeds"
			exit()


		if art not in art_entities.keys():
			art_entities[art] = []
			response = alchemyapi.entities('url', art, {'sentiment': 1})
			count = count + 1
			if response['status'] == 'OK':
				for entities in response['entities']:
					# print('text: ', entities['text'].encode('utf-8'))
					key = str(entities['text'].encode('utf-8')).replace("'","")
					art_entities[art].append(key)

					rel_dict = {}

					rel_dict['type'] = entities['type']
					rel_dict['relevance'] = entities['relevance']
					rel_dict['sentiment'] = entities['sentiment']['type']
					if 'score' in entities['sentiment']:
						rel_dict['sentiment_score'] = entities['sentiment']['score']
					
					entities_node = """ MERGE (entities:Entities{text:'"""+key+"""'})
					Return entities;
					"""
					at_entities = graph.cypher.execute(entities_node)
					pth = Path(article.one,("has_entity",rel_dict),at_entities.one)
					graph.create(pth)
					

	return "200"

# try:
# 	first = leads()
# except Exception,e:
# 	print e
# 	pass

# try:
# 	second = articles_func()
# except Exception,e:
# 	print e
# 	pass

try:
	third = key_entity()
except Exception,e:
	print e
	pass
