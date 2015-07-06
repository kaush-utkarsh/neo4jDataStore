import csv, json, re
from py2neo import Graph, Path
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
		articles[(str(row['Name']).replace("'",""))]='https://www.brighttalk.com'+row['Entry Page']
csvfile.close()	

csvfile = open('MarketoLeads.csv')

reader = csv.DictReader(csvfile)

for row in reader:
	name = (str(row['First_Name']+' '+row['Last_Name']).replace("'",""))
	email = row['Email_Address']
	company = (str(row['Company_Name']).replace("'",""))
	industry = (str(row['Industry']).replace("'",""))
	iD = row['Id']
	job = (str(row['Job_Title']).replace("'",""))

	individual_node = """ Create (idividual:Individuals{id:'"""+iD+"""',name:'"""+name+"""',email:'"""+email+"""'})
	Return idividual;
	"""
	individual = graph.cypher.execute(individual_node)

	company_node = """ Create (company:Company{name:'"""+company+"""'})
	Return company;
	"""
	company = graph.cypher.execute(company_node)

	job_node = """ Create (job:Job_Title{name:'"""+job+"""'})
	Return job;
	"""
	job = graph.cypher.execute(job_node)
	
	industry_node = """ Create (industry:Industry{name:'"""+industry+"""'})
	Return industry;
	"""
	industry = graph.cypher.execute(industry_node)

	graph.create(Path(individual.one,"works at",company.one))
	graph.create(Path(individual.one,"works as",job.one))
	graph.create(Path(individual.one,"is in Industry",industry.one))
	
	if name in articles.keys():
		article_node = """ Create (article:Article{url:'"""+articles[name]+"""'})
		Return article;
		"""
		article = graph.cypher.execute(article_node)

		graph.create(Path(individual.one,"reads",article.one))

