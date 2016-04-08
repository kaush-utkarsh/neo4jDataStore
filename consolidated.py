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
def leads(csvFilePath, config_file, act):
	
	config = ConfigParser.ConfigParser()
	config.read(config_file)

	nodes = config.options("nodes")
	relation = config.options("relation")

	# fetch from csv and store in graph
	print "Storing Leads"
	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	csvfile = open(csvFilePath)

	reader = csv.DictReader(csvfile)

	reslt = {}
	indi_count = 0

	for row in reader:
		try:
			nodeList={}
			for n in nodes:
				node_atr = config.get("nodes",n)
				attr = json.loads(node_atr)
				individual_node = ""
				try:
					if bool(attr):
						individual_node = """ MERGE ("""+act+""":"""+n+"""{"""

						for attr_key in attr.keys():
							individual_node=individual_node+str(attr_key)+""" :'"""+(str(row[attr[attr_key]]).replace("'",""))+"""', """
						individual_node = individual_node.strip(", ")+"""})
						Return """+act+""";
						"""
						individual = graph.cypher.execute(individual_node)		
						nodeList[n]=individual
						indi_count=indi_count+1
				except Exception,ee:
					pass

			for n in relation:
				rel_atr = config.get("relation",n)
				attr = json.loads(rel_atr)
				try:
					if bool(attr):

						start_node = attr['start_node']
						end_node = attr['end_node']
						attributes = attr['attributes']
	
						rel_dict={}

						for u in attributes.keys():

							rel_dict[u] = (str(row[attributes[u]]).replace("'",""))
						
						if len(list(graph.match(start_node=nodeList[start_node].one,end_node=nodeList[end_node].one, rel_type=(n,rel_dict)))) == 0:
							graph.create(Path(nodeList[start_node].one,(n,rel_dict),nodeList[end_node].one))
				except Exception,ee:
					pass
	reslt[act] = str(indi_count)

	return reslt

def key_entity():

	print "storing keywords"
	# Create the AlchemyAPI Object
	alchemyapi = AlchemyAPI()

	art_keywords = {}
	art_entities = {}
	
	count = 0 

	graph = Graph("http://neo4j:root@localhost:7474/db/data/")

	article_query =  """MATCH (n:article) 
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

		article_node = """ MATCH (article:article{url:'"""+art+"""'})
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

	csvFilePath = sys.argv[2]
	config_file = sys.argv[3]
	try:
		resultDict = leads(csvFilePath,config_file,action)
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


# emailUpdate(from_addr,to_addr,msgBod)