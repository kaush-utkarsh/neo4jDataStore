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
python consolidated.py leads path_to_the_file
```
Where the path_to_the_file is the path to the CSV file



The next part is the addition of articles to the db.

The command to save the articles is:

```
python consolidated.py articles path_to_the_file
```
Where the path_to_the_file is the path to the CSV file



The last part is the keywords and entity extraction to the db.

The command to save the articles is:

```
python consolidated.py keywords
```


After every job a notification mail is sent to the admin explaining the action taken, file handles during the action, the number of tuples worked on and the total duration of the whole job. 
