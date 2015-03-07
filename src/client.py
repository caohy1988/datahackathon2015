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
  try:
      return json.loads("".join(resp).strip())
  except:
      return "".join(resp).strip()

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--port-num", type=int, default=8080)
  parser.add_argument("query")
  args = vars(parser.parse_args())
  resp = query(args['query'], port_num=args['port_num'])
  print json.dumps(resp, indent=4, sort_keys=True)

if "__main__" == __name__:
  main()
