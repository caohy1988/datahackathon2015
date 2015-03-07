#!/usr/bin/python
import sys
import socket
import argparse
import json

def query(query_string, port_num=8080):
  HOST = 'localhost'    # The remote host
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((HOST, port_num))
  s.sendall(query_string + "\n")
  resp = list()
  while True:
    piece = s.recv(1024)
    resp.append(piece)
    if not piece:
      resp.pop()
      break
  s.close()
  data = json.loads("".join(resp).strip())
  try:
    print json.dumps(data, indent=4, sort_keys=True)
  except ValueError:
    print data

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--port-num", type=int, default=8080)
  parser.add_argument("query")
  args = vars(parser.parse_args())
  query(args['query'], port_num=args['port_num'])

if "__main__" == __name__:
  main()
