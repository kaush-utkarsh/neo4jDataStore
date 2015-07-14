import csv, json, re
from py2neo import Graph, Path
import json, sys



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

				graph.create(Path(individual.one,"works_at",company.one))
			
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
		pass

