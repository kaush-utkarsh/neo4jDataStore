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

The command to save the leads/articles is:

```
python consolidated.py action path_to_the_file path_to_config_file
```
Where the path_to_the_file is the path to the CSV file
And the action can be either "leads" or "article"

this will work for both the article and individual lead csvs

The last part is the keywords and entity extraction to the db.

The command to save the articles is:

```
python consolidated.py keywords
```


### config file is used to change the given csv into the standard csv format.

a config file is a file with an ".ini" extension

there will be two ini files to be used (one for leads and another for articles)


the config file for leads: would look like..

```
[nodes]

user:{ "id":"Id", "fname":"First_Name", "lname":"Last_Name", "email":"Email_Address" }
country:{}
industry:{"name":"Industry”}

[relation]
works_in:{"start_node":"user","end_node":"industry" attributes:{"job":"Job_Title","company":"Company_Name"}}


```

After every job a notification mail is sent to the admin explaining the action taken, file handles during the action, the number of tuples worked on and the total duration of the whole job. 
