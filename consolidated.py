import csv, json, re
from py2neo import Graph, Path
import json, sys
from alchemyapi import AlchemyAPI
from smtplib import SMTP
import ConfigParser
import datetime

from_addr = "Utkarsh Kaushik<ukaushik@algoscale.com>"
to_addr = "Utkarsh Kaushik<ukaushik@algoscale.com>, Neeraj Agarwal<neeraj@algoscale.com>"

start = datetime.datetime.now().strftime( "%d/%m/%Y %H:%M" )

csvFilePath = ''
config_file = ''
def leads(csvFilePath, config_file):
	
	config = ConfigParser.ConfigParser()
	config.read(config_file)
	userCol = config.options("user")
	industryCol = config.options("industry")
	communityCol = config.options("community")
	countryCol = config.options("country")
	
	# parse config file 
	config_l_id = config.get("user","Id")
	config_email = config.get("user","email")
	config_industry = config.get("industry","industry")
	config_community = config.get("community","community")
	config_sub_community = config.get("community","sub_community")
	config_country = config.get("country","country")



	# fetch from csv and store in graph
	print "Storing Leads"
	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	csvfile = open(csvFilePath)

	reader = csv.DictReader(csvfile)

	reslt = {}
	indi_count = 0

	for row in reader:
		try:
			email = (str(row[config_email]).replace("'",""))
			industry = (str(row[config_industry]).replace("'",""))
			community = (str(row[config_community]).replace("'",""))
			sub_community = (str(row[config_sub_community]).replace("'",""))
			country = (str(row[config_country]).replace("'",""))
			

			iD = row[config_l_id]

			if email!='' and iD!='':

				try:

					individual_node = """ MERGE (idividual:Individuals{id:'"""+iD+"""',email:'"""+email+"""'})
					Return idividual;
					"""
					individual = graph.cypher.execute(individual_node)

					for u in userCol:

						tmp = config.get("user",u)
		
						individual.one.properties[u] = str(row[tmp]).replace("'","")

					individual.one.push()				
				except Exception,e:
					print e
					continue
				# exit()
				indi_count = indi_count + 1

				if industry != '':
					company_node = """ MERGE (industry:Industry{name:'"""+industry+"""'})
					Return industry;
					"""
					company = graph.cypher.execute(company_node)
					
					if len(list(graph.match(start_node=individual.one,end_node=company.one, rel_type="is_in_Industry"))) == 0:
						graph.create(Path(individual.one,"is_in_Industry",company.one))
				
				if country != '':
					company_node = """ MERGE (country:Country{name:'"""+country+"""'})
					Return country;
					"""
					company = graph.cypher.execute(company_node)
					
					if len(list(graph.match(start_node=individual.one,end_node=company.one, rel_type="lives_in_country"))) == 0:
						graph.create(Path(individual.one,"lives_in_country",company.one))
				

				if community!= '':
					community_node = """ MERGE (community:Community{name:'"""+community+"""'})
					Return community;
					"""
					community = graph.cypher.execute(community_node)

					if len(list(graph.match(start_node=individual.one,end_node=community.one, rel_type="belongs_to_community"))) == 0:
						graph.create(Path(individual.one,"belongs_to_community",community.one))
				
				if sub_community!= '':
					sub_community_node = """ MERGE (sub_community:Sub_Community{name:'"""+sub_community+"""'})
					Return sub_community;
					"""
					sub_community = graph.cypher.execute(sub_community_node)

					if len(list(graph.match(start_node=individual.one,end_node=sub_community.one, rel_type="belongs_to_sub_community"))) == 0:
						graph.create(Path(individual.one,"belongs_to_sub_community",sub_community.one))

				
		except Exception,e:
			raise
			exit()

	reslt['individuals'] = str(indi_count)

	return reslt

def articles_func(csvFilePath,config_file):
	

	config = ConfigParser.ConfigParser()
	config.read(config_file)

	articleCol = config.options("articles")
	# parse config file 
	config_url = config.get("articles","url")
	config_employee_name = config.get("articles","employee_name")



	print "storing articles"
	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	csvfile = open(csvFilePath)

	reader = csv.DictReader(csvfile)
	articles = {}
	art_count = 0
	for row in reader:

		try:

			if row[config_employee_name]!='':	
				name = (str(row[config_employee_name]).replace("'",""))
				art = 'https://www.brighttalk.com'+row[config_url]

				individual_node = """ MERGE (idividual:Individuals{name:'"""+name+"""'})
				Return idividual;
				"""
				individual = graph.cypher.execute(individual_node)

				article_node = """ MERGE (article:Article{url:'"""+art+"""',processed:'no'})
				Return article;
				"""
				article = graph.cypher.execute(article_node)
				art_count = art_count + 1

				rel_dict={}


				for u in articleCol:

					tmp = config.get("articles",u)
		
					rel_dict[u] = (str(row[tmp]).replace("'",""))
				
				if len(list(graph.match(start_node=individual.one,end_node=article.one, rel_type=("read",rel_dict)))) == 0:
					graph.create(Path(individual.one,("read",rel_dict),article.one))


		except Exception,e:
			print e
			pass

	return {'articles': str(art_count)}


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

	keyword_count = 0
	entity_count = 0
	art_count = 0
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
			art_count = art_count + 1
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

					if len(list(graph.match(start_node=article.one,end_node=at_keywords.one, rel_type=("has_keyword",rel_dict)))) == 0:
						pth = Path(article.one,("has_keyword",rel_dict),at_keywords.one)
						graph.create(pth)
						keyword_count = keyword_count + 1


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
					if len(list(graph.match(start_node=article.one,end_node=at_entities.one, rel_type=("has_entity",rel_dict)))) == 0:		
						at_entities = graph.cypher.execute(entities_node)
						pth = Path(article.one,("has_entity",rel_dict),at_entities.one)
						graph.create(pth)
						
						entity_count  = entity_count + 1

	return {'articles':str(art_count),'keywords':str(keyword_count),'entities':str(entity_count)}


def emailUpdate(from_addr,to_addr,message):
	smtp = SMTP()
	smtp.connect('smtp.mailgun.org', 587)
	smtp.login('postmaster@nogpo.com', 'f63f5059d3255612cfae4470ed9b34ed')
	smtp.sendmail(from_addr, to_addr, message)
	smtp.quit()


if len(sys.argv)<=1:
	print "insufficient parameters"
	msg = """
	Hi, \n 
	This is an update related to the job triggered on """+str(start)+""", for the insertion of data in your Neo4J graph base. \n
	The code could not complete the run because of insufficient parameters supplied to the query \n 
	Regards """

	subj = "Update for uLytics job on Neo4J"


	msgBod = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, start, msg )
	
	emailUpdate(from_addr,to_addr,msgBod)
	
	exit()
else:
	action = sys.argv[1]	

	if action == 'leads':
		csvFilePath = sys.argv[2]
		config_file = sys.argv[3]
		try:
			resultDict = leads(csvFilePath,config_file)
		except Exception,e:
			print e
			msg = """
			Hi, \n 
			This is an update related to the job triggered on """+str(start)+""", for the insertion of data in your Neo4J graph base. \n
			The code could not complete the run because of insufficient parameters supplied to the query \n 
			Regards """

			subj = "Update for uLytics job on Neo4J"


			msgBod = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, start, msg )

			emailUpdate(from_addr,to_addr,msgBod)

			exit()
			pass

	elif action == 'articles':
		csvFilePath = sys.argv[2]
		config_file = sys.argv[3]
		try:
			resultDict = articles_func(csvFilePath,config_file)
		except Exception,e:
			print e
			msg = """
			Hi, \n 
			This is an update related to the job triggered on """+str(start)+""", for the insertion of data in your Neo4J graph base. \n
			The code could not complete the run because of insufficient parameters supplied to the query \n 
			Regards """

			subj = "Update for uLytics job on Neo4J"


			msgBod = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, start, msg )

			emailUpdate(from_addr,to_addr,msgBod)

			exit()
			pass

	else:
		try:
			resultDict = key_entity()
		except Exception,e:
			print e
			pass

end = datetime.datetime.now().strftime( "%d/%m/%Y %H:%M" )

msg = """
Hi, \n 
This is an update related to the job triggered on """+str(start)+""", for the insertion of data in your Neo4J graph base. \n
The Job ended at: """+str(end)+""". \n 
The code run took place for the action related to: '"""+action+"""' \n 
It ran for """

for key in resultDict.keys():
	msg = msg + resultDict[key] + " " + key + ", "

msg = msg.strip(', ')
msg = msg + " \n\nRegards\n"
subj = "Update for uLytics job on Neo4J"


msgBod = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, end, msg )


emailUpdate(from_addr,to_addr,msgBod)