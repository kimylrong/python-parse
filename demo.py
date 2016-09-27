import MySQLdb
import os.path
import codecs
from datetime import datetime
from bs4 import BeautifulSoup


# read content
content = ""
with open('7654716-1.bin', 'r') as lastfile:
    content = lastfile.read()

# read rule
rules_str = []
with codecs.open('rule.txt', encoding='utf-8') as rulefile:
    rules_str = rulefile.readlines()
rules = [rule.strip() for rule in rules_str]
print rules

def process_html(html, rules):
    data = []
    soup = BeautifulSoup(html, 'html.parser')
    print soup.original_encoding
    parags = soup.find_all("img", recursive=False)
    print len(parags)
    for parag in parags:
        if not parag.string:
            continue
        unicode_parag = unicode(parag)
        is_target = '0'
        for rule in rules:
            if rule in unicode_parag:
                is_target = '1'
                break                
        data.append(is_target)
    return data

print content
process_html(content, rules)