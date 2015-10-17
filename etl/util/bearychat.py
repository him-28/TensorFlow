#!/usr/bin/python
# -*- coding: utf-8 -*- #
############################################################################
## 
## Copyright (c) 2015 hunantv.com, Inc. All Rights Reserved
## $Id: bearychat.py,v 0.0   dongjie Exp $ 
## 
############################################################################
#
###
# # @file   bearychat.py 
# # @author dongjie<dongjie@e.hunantv.com>
# # @date  
# # @brief 
# #  
# ##
import httplib
import urllib
import json

def send_message(title, text):
    #params_str = ({"text": message, "attachments": [{"title":"This attachments Test", "text": "pls ignore this msg", "color":"#ffa500"}]})
    params_str = ({"text": "", "attachments": [{"title": title, "text": text, "color":"#ffa500"}]})
    params = json.JSONEncoder().encode(params_str)
    headers = {"Content-Type" : "application/json"}
    bearychat_url = "https://hook.bearychat.com/=bw7by/incoming/c8a733da6e453ec05e5bb2883d8af05e"

    conn = httplib.HTTPConnection("hook.bearychat.com")
    conn.request(method="POST", url=bearychat_url, body=params, headers=headers)
    response = conn.getresponse()
    return response.status

## vim: set ts=2 sw=2: #

if __name__ == '__main__':
  send_message("hello world", "what's this")
