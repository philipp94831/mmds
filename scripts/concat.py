from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir('final') if isfile(join('final', f))]
training = [f for f in onlyfiles if f.startswith('training')]
test = [f for f in onlyfiles if f.startswith('test')]
with open('final/training.txt', 'w') as outfile:
  for fname in training:
    with open('final/' + fname) as infile:
      for line in infile:
        outfile.write(line)
with open('final/test.txt', 'w') as outfile:
  for fname in test:
    with open('final/' + fname) as infile:
      for line in infile:
        outfile.write(line)