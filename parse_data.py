
import pandas as pd
import json
import os
import bz2
import itertools
import datetime
import tqdm
import tarfile
from multiprocessing import Pool

def clean_file(member):
    if '.bz2' in str(member):

        f = tr.extractfile(member)

        with bz2.open(f, "rt") as bzinput:
            dicts = []
            for i, line in enumerate(bzinput):
                line = line.replace('"name"}', '"name":" "}')
                dat = json.loads(line)
                dicts.append(dat)

        bzinput.close()
        f.close()
        del f, bzinput

        processed = dicts[0]
        return processed

    else:
        pass


# Open tar file and get contents (members)
tr = tarfile.open('data.tar')
members = tr.getmembers()
num_files = len(members)


# Apply the clean_file function in series
i=0
processed_files = []
for m in members:
    processed_files.append(clean_file(m))
    i+=1
    print('done '+str(i)+'/'+str(num_files))


# Apply the clean_file function in parallel
if __name__ == '__main__':
   with Pool(2) as p:
      processed_files = list(tqdm.tqdm(p.imap(clean_file, members), total=num_files))





