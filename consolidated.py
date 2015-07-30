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

	# parse config file 
	config_last_name = config.get("leads","last_name")
	config_first_name = config.get("leads","first_name")
	config_l_id = config.get("leads","l_id")
	config_job = config.get("leads","job")
	config_company = config.get("leads","company")
	config_email = config.get("leads","email")
	config_phone = config.get("leads","phone")
	config_lead_score = config.get("leads","lead_score")
	config_lead_source = config.get("leads","lead_source")
	config_updated_at = config.get("leads","updated_at")
	config_job_function = config.get("leads","job_function")
	config_industry = config.get("leads","industry")
	config_community = config.get("leads","community")
	config_sub_community = config.get("leads","sub_community")



	# fetch from csv and store in graph
	print "Storing Leads"
	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	csvfile = open(csvFilePath)

	reader = csv.DictReader(csvfile)

	reslt = {}
	indi_count = 0

	for row in reader:
		try:
			name = (str(row[config_first_name]+' '+row[config_last_name]).replace("'",""))
			email = (str(row[config_email]).replace("'",""))
			company = (str(row[config_company]).replace("'",""))
			industry = (str(row[config_industry]).replace("'",""))
			community = (str(row[config_community]).replace("'",""))
			sub_community = (str(row[config_sub_community]).replace("'",""))
			
			phone = (str(row[config_phone]).replace("'",""))
			lead_score = (str(row[config_lead_score]).replace("'",""))
			lead_source = (str(row[config_lead_source]).replace("'",""))
			updated_at = (str(row[config_updated_at]).replace("'",""))
			

			iD = row[config_l_id]
			job = (str(row[config_job]).replace("'",""))

			if name!='' and email!='' and iD!='':

				individual_node = """ MERGE (idividual:Individuals{id:'"""+iD+"""',name:'"""+name+"""',email:'"""+email+"""'})
				Return idividual;
				"""
				individual = graph.cypher.execute(individual_node)


				individual.one.properties["phone"] = phone
				individual.one.properties["lead_source"] = lead_source
				individual.one.properties["lead_score"] = lead_score
				individual.one.properties["updated_at"] = updated_at

				individual.one.push()				

				indi_count = indi_count + 1

				if company!= '':
					company_node = """ MERGE (company:Company{name:'"""+company+"""'})
					Return company;
					"""
					company = graph.cypher.execute(company_node)
					
					if len(list(graph.match(start_node=individual.one,end_node=company.one, rel_type="works_at"))) == 0:
						graph.create(Path(individual.one,"works_at",company.one))
				
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
				

				if job != '':
					job_node = """ MERGE (job:Job_Title{name:'"""+job+"""'})
					Return job;
					"""
					job = graph.cypher.execute(job_node)

					if len(list(graph.match(start_node=individual.one,end_node=job.one, rel_type="works_as"))) == 0:
						graph.create(Path(individual.one,"works_as",job.one))


				
				if industry != '':
					industry_node = """ MERGE (industry:Industry{name:'"""+industry+"""'})
					Return industry;
					"""
					industry = graph.cypher.execute(industry_node)

					if len(list(graph.match(start_node=individual.one,end_node=industry.one, rel_type="is_in_Industry"))) == 0:
						graph.create(Path(individual.one,"is_in_Industry",industry.one))
				
		except Exception,e:
			raise
			exit()

	reslt['individuals'] = str(indi_count)

	return reslt

def articles_func(csvFilePath,config_file):
	

	config = ConfigParser.ConfigParser()
	config.read(config_file)

	# parse config file 
	config_lead = config.get("articles","lead")
	config_first_visit = config.get("articles","first_visit")
	config_referer_url = config.get("articles","referer_url")
	config_url = config.get("articles","url")
	config_employee_name = config.get("articles","employee_name")
	config_employee_job = config.get("articles","employee_job")
	config_employee_company = config.get("articles","employee_company")
	config_inferred_company = config.get("articles","inferred_company")
	config_inferred_country = config.get("articles","inferred_country")
	config_inferred_state = config.get("articles","inferred_state")
	config_inferred_city = config.get("articles","inferred_city")
	config_inferred_phone_area = config.get("articles","inferred_phone_area")
	config_last_visit = config.get("articles","last_visit")



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

				rel_dict["lead"] = (str(row[config_lead]).replace("'","")) 
				rel_dict["first_visit"] = (str(row[config_first_visit]).replace("'","")) 
				rel_dict["referer_url"] = (str(row[config_referer_url]).replace("'","")) 
				rel_dict["employee_job"] = (str(row[config_employee_job]).replace("'","")) 
				rel_dict["employee_company"] = (str(row[config_employee_company]).replace("'","")) 
				rel_dict["inferred_company"] = (str(row[config_inferred_company]).replace("'","")) 
				rel_dict["inferred_country"] = (str(row[config_inferred_country]).replace("'","")) 
				rel_dict["inferred_state"] = (str(row[config_inferred_state]).replace("'","")) 
				rel_dict["inferred_city"] = (str(row[config_inferred_city]).replace("'","")) 
				rel_dict["inferred_phone_area"] = (str(row[config_inferred_phone_area]).replace("'","")) 
				rel_dict["last_visit"] = (str(row[config_last_visit]).replace("'","")) 
				
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