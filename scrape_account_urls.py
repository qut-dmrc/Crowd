import pandas as pd
import re

filename = "Навальный 17-24.csv"
listname = "RT3_group"

df = pd.read_csv(filename)

# if isinstance(url, str):
#     f.write(url+"\n")
# if (re.match("https://www.facebook.com/.*/posts",url)):
#     f.write(re.match("https://www.facebook.com/.*/posts",url).group(0).replace("/posts","")+"\n")
urls = df['URL'].tolist()
url_set = set()
for url in urls:
    if isinstance(url, str):
        second_to_last_slash = url.rfind('/', 0, url.rfind('/'))
        url = url[:second_to_last_slash]
        url_set.add(url)

with open('batch_upload_template.csv','w') as o:
    with open("url_shortened.txt",'w') as s:
        o.write("Page or Account URL,List\n") # header
        for url in url_set:
            o.write("{},{}\n".format(url,listname))
            s.write(url+"\n")
        