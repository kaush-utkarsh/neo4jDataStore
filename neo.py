import csv, json, re
from py2neo import Graph, Path
from alchemyapi import AlchemyAPI
import json, sys

# Create the AlchemyAPI Object
alchemyapi = AlchemyAPI()


graph = Graph("http://neo4j:root@localhost:7474/db/data/")

# for name in ["Alice", "Bob", "Carol"]:
#     tx.append("CREATE (person:Person {name:{name}}) RETURN person", name=name)
# alice, bob, carol = [result.one for result in tx.commit()]

# friends = Path(alice, "KNOWS", bob, "KNOWS", carol)
# graph.create(friends)

csvfile = open('Web_Page_Activity.csv')

reader = csv.DictReader(csvfile)
articles = {}
for row in reader:
	if row['Name']!='':	
		if (str(row['Name']).replace("'","")) in articles.keys():
			articles[(str(row['Name']).replace("'",""))].append('https://www.brighttalk.com'+row['Entry Page'])
		else:
			articles[(str(row['Name']).replace("'",""))] = []
			articles[(str(row['Name']).replace("'",""))].append('https://www.brighttalk.com'+row['Entry Page'])
			
csvfile.close()	

csvfile = open('MarketoLeads.csv')

reader = csv.DictReader(csvfile)

art_keywords = {}

for row in reader:
	name = (str(row['First_Name']+' '+row['Last_Name']).replace("'",""))
	email = row['Email_Address']
	company = (str(row['Company_Name']).replace("'",""))
	industry = (str(row['Industry']).replace("'",""))
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

			graph.create(Path(individual.one,"works at",company.one))


		if job != '':
			job_node = """ MERGE (job:Job_Title{name:'"""+job+"""'})
			Return job;
			"""
			job = graph.cypher.execute(job_node)

			graph.create(Path(individual.one,"works as",job.one))


		
		if industry != '':
			industry_node = """ MERGE (industry:Industry{name:'"""+industry+"""'})
			Return industry;
			"""
			industry = graph.cypher.execute(industry_node)

			graph.create(Path(individual.one,"is in Industry",industry.one))

		
		
		if name in articles.keys():
			for art in articles[name]:
				article_node = """ MERGE (article:Article{url:'"""+art+"""'})
				Return article;
				"""
				article = graph.cypher.execute(article_node)

				graph.create(Path(individual.one,"read",article.one))

				if art not in art_keywords.keys():
					art_keywords[art] = []
					response = alchemyapi.keywords('url', art, {'sentiment': 1})
					if response['status'] == 'OK':
						for keyword in response['keywords']:
							# print('text: ', keyword['text'].encode('utf-8'))
							key = str(keyword['text'].encode('utf-8'))
							art_keywords[art].append(key)

							keyword_node = """ MERGE (keyword:Keywords{text:'"""+key+"""'})
							Return keyword;
							"""
							at_keywords = graph.cypher.execute(keyword_node)

							graph.create(Path(article.one,"has keyword",at_keywords.one))
						# exit()

