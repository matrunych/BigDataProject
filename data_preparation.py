import json
import time
from datetime import datetime

import requests as req
from write_file import upload_from_bucket, upload_to_bucket

to_write = []
resp = req.get("https://stream.meetup.com/2/rsvps", stream=True)
i = 0
for chunk in resp.iter_content(chunk_size=10000):
    if chunk:
        try:
            i += 1
            line = json.loads(chunk.decode('UTF-8').replace("\\", ""))
            line1 = dict()
            line1['country'] = line["group"]["group_country"]
            line1['group_city'] = line['group']['group_city']
            line1['group_name'] = line['group']['group_name']
            line1['group_topics'] = [x['topic_name'] for x in line['group']['group_topics']]
            line1['mtime'] = line['mtime']/1000
            date_time = datetime.fromtimestamp(line1['mtime'])

            tm = str(int(date_time.strftime("%H")))
            to_write.append(line1)
            file = open(tm + '.json', 'a')
            if i % 20 == 0:
                res = upload_from_bucket('temporary_data/' + str(tm) + '.json')
                for k in to_write:
                    res += json.dumps(k) + '\n'
                to_write = []
                upload_to_bucket('temporary_data/' + str(tm) + '.json', res)
        except json.decoder.JSONDecodeError:
            pass


resp.close()
