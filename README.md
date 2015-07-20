# uLytics script Walkthrough

The main file to be executed: consolidated.py

The file contains functions to
 * Save leads from a CSV
 * Save articles data from a csv
 * Extract keywords and entities of respective articles
 * Send a notification mail at the end of the whole run

The basic commad to execute a python file: 

```
python consolidated.py
```

To execute any particular function you need to pass command line arguments, specifying the action and the concerned file.

Lets start from the very begining:

We begin with the addition of leads to the db.

The command to save the leads is:

```
python consolidated.py leads path_to_the_file path_to_config_file
```
Where the path_to_the_file is the path to the CSV file



The next part is the addition of articles to the db.

The command to save the articles is:

```
python consolidated.py articles path_to_the_file path_to_config_file
```
Where the path_to_the_file is the path to the CSV file


The last part is the keywords and entity extraction to the db.

The command to save the articles is:

```
python consolidated.py keywords
```


### config file is used to change the given csv into the standard csv format.

a config file is a file with an ".ini" extension

there will be two ini files to be used (one for leads and another for articles)


the config file for leads: leads.ini would look like..

```
[leads]
l_id: Id
last_name: Last Name
first_name: First Name
job: Job Title
company: Company Name
email: Email Address
phone: Phone Number
lead_score: Lead Score
lead_source: Lead Source
updated_at: Updated At
job_function: Job Function
industry: Industry
community: Community Segment
sub_community: Sub Community

```
Note: the header '[leads]' is part of ini and not supposed to be changed without reflective changes in code
here the values in the left side of ':' are restrictive and constants, while the right side values are the column header texts of input csv(MarketoLeads csv)


Similarly in case of articles the config file: articles.ini would look like..

```
[articles]
lead: Lead
first_visit: First Visit (America/Los_Angeles)
referer_url: HTTP Referer
url: Entry Page
employee_name: Name
employee_job: Job Title
employee_company: Company
inferred_company: Inferred Company or ISP
inferred_country: Inferred Country
inferred_state: Inferred State/Region
inferred_city: Inferred City
inferred_phone_area: Inferred Phone Area Code
last_visit: Last Visit (America/Los_Angeles)

```
Note: the header '[articles]' is part of ini and not supposed to be changed without reflective changes in code
here the values in the left side of ':' are restrictive and constants, while the right side values are the column header texts of input csv(Web_Page_activity csv)


After every job a notification mail is sent to the admin explaining the action taken, file handles during the action, the number of tuples worked on and the total duration of the whole job. 
