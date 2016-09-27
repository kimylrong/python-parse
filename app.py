import MySQLdb
import os.path
import codecs
from datetime import datetime
from bs4 import BeautifulSoup

db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="test", charset='utf8')
print "db connectd"
cur = db.cursor()


# read last_time
last_time = ""
if os.path.isfile('last_time.txt'):
    with open('last_time.txt', 'r') as lastfile:
        last_time = lastfile.read().replace('\n', '')
if not last_time:
    last_time = "2010-01-01 00:00:01"
print "last do time: %s" % last_time
start_time = datetime.now()

# read rule
rules_str = []
with codecs.open('rule.txt', encoding='utf-8') as rulefile:
    rules_str = rulefile.readlines()
rules = [rule.strip() for rule in rules_str]
print rules

# query record count, caculate page
size_per_page = 5
cur.execute("SELECT count(*) FROM html_parse WHERE modified_time>%s", (last_time,))
count_record = cur.fetchone()[0]
count_page = count_record/size_per_page
left = count_record % size_per_page
if not left == 0:
    count_page += 1
print "count_record=%s, count_page=%s, size_per_page=%s" % (count_record, count_page, size_per_page)

def process_html(html, rules):
    data = []
    soup = BeautifulSoup(html, 'html.parser')
    print soup.original_encoding
    parags = soup.find_all("p", recursive=False)
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

# do page by page
if count_page > 0:
    with codecs.open('record.txt', 'a', encoding='utf-8') as record_file:
        for page in range(count_page):
            print "process page %s" % (page+1)
            cur.execute("SELECT * FROM html_parse WHERE modified_time>%s LIMIT %s, %s", (last_time, page*size_per_page, size_per_page))
            # print all the first cell of all the rows
            for row in cur.fetchall():
                id = row[0]
                title = row[1]
                content = row[2]
                result = process_html(content, rules)
                if len(result)>0:
                    record = str(id) + "=" + ",".join(result)+"\n"	
                    print result
                    record_file.write(record)

# do statistic
with codecs.open('record.txt', 'r', encoding='utf-8') as record_file:
    forward_1=0
    forward_2=0
    forward_3=0
    backward_1=0
    backward_2=0
    backward_3=0
    other=0
    for line in record_file.readlines():
        index = line.find('=')
        id = line[:index]
        data = line[index+1:-1]
        data_array = data.split(',')
        print data_array
        size = len(data_array)
        if size==1:
            forward_1 += int(data_array[0])
        elif size==2:
            forward_1 += int(data_array[0])
            backward_1 += int(data_array[-1])
        elif size==3:
            forward_1 += int(data_array[0])
            forward_2 += int(data_array[1])
            backward_1 += int(data_array[-1])
        elif size==4:
            forward_1 += int(data_array[0])
            forward_2 += int(data_array[1])
            backward_1 += int(data_array[-1])
            backward_2 += int(data_array[-2])
        elif size==5:
            forward_1 += int(data_array[0])
            forward_2 += int(data_array[1])
            forward_3 += int(data_array[2])
            backward_1 += int(data_array[-1])
            backward_2 += int(data_array[-2])
        elif size==6:
            forward_1 += int(data_array[0])
            forward_2 += int(data_array[1])
            forward_3 += int(data_array[2])
            backward_1 += int(data_array[-1])
            backward_2 += int(data_array[-2])
            backward_3 += int(data_array[-3])
        else:
            forward_1 += int(data_array[0])
            forward_2 += int(data_array[1])
            forward_3 += int(data_array[2])
            backward_1 += int(data_array[-1])
            backward_2 += int(data_array[-2])
            backward_3 += int(data_array[-3])
            for item in data_array[4:-3]:
                other+=int(item)

    total = forward_1 + forward_2 + forward_3 + backward_1 + backward_2 + backward_3 + other
    print "total count: %s" % total
    print "parag     1:   count=%-6s    percent=%-5s" % (forward_1, round(forward_1/float(total)*100, 2))
    print "parag     2:   count=%-6s    percent=%-5s" % (forward_2, round(forward_2/float(total)*100, 2))
    print "parag     3:   count=%-6s    percent=%-5s" % (forward_3, round(forward_3/float(total)*100, 2))
    print "parag    -1:   count=%-6s    percent=%-5s" % (backward_1, round(backward_1/float(total)*100, 2))
    print "parag    -2:   count=%-6s    percent=%-5s" % (backward_2, round(backward_2/float(total)*100, 2))
    print "parag    -3:   count=%-6s    percent=%-5s" % (backward_3, round(backward_3/float(total)*100, 2))
    print "parag other:   count=%-6s    percent=%-5s" % (other, round(other/float(total)*100, 2))


# record last time			
with open('last_time.txt', 'w+') as lastfile:
    now_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    lastfile.write(now_str)

db.close()