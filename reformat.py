#!/usr/bin/python
import simplejson as json
import csv
import sys
import argparse

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('infile')
  args = vars(parser.parse_args())
  with open(args['infile']) as infile:
    reader = csv.DictReader(infile)
    for line in reader:
      for key, val in line.items():
        if not val or (val in ['Unspecified']):
          line.pop(key)
      print json.dumps(line, sort_keys=True)

if "__main__" == __name__:
  main()
