from __future__ import print_function
from alchemyapi import AlchemyAPI
import json, sys

# Create the AlchemyAPI Object
alchemyapi = AlchemyAPI()
if len(sys.argv)<=1:
	print("url expected as a parameter")
	exit()

# url = 'http://www.npr.org/2013/11/26/247336038/dont-stuff-the-turkey-and-other-tips-from-americas-test-kitchen'
url = sys.argv[1]
# print(url)
# exit()

print('')
print('')
print('############################################')
print('#   Entity Extraction Example              #')
print('############################################')
print('')
print('')

response = alchemyapi.entities('url', url, {'sentiment': 1})

if response['status'] == 'OK':
    # print('## Response Object ##')
    # print(json.dumps(response, indent=4))

    # print('')
    print('## Entities ##')
    for entity in response['entities']:
        print('text: ', entity['text'].encode('utf-8'))
        print('type: ', entity['type'])
        print('relevance: ', entity['relevance'])
        print('sentiment: ', entity['sentiment']['type'])
        if 'score' in entity['sentiment']:
            print('sentiment score: ' + entity['sentiment']['score'])
        print('')
else:
    print('Error in entity extraction call: ', response['statusInfo'])

print('')
print('')
print('')
print('############################################')
print('#   Keyword Extraction Example             #')
print('############################################')
print('')
print('')

response = alchemyapi.keywords('url', url, {'sentiment': 1})

if response['status'] == 'OK':
    # print('## Response Object ##')
    # print(json.dumps(response, indent=4))

    # print('')
    print('## Keywords ##')
    for keyword in response['keywords']:
        print('text: ', keyword['text'].encode('utf-8'))
        print('relevance: ', keyword['relevance'])
        print('sentiment: ', keyword['sentiment']['type'])
        if 'score' in keyword['sentiment']:
            print('sentiment score: ' + keyword['sentiment']['score'])
        print('')
else:
    print('Error in keyword extaction call: ', response['statusInfo'])

