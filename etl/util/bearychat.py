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

def new_send_message(text, margdown=True, channel=u'广告-数据', at_title='', at_text='', at_color='#ffa500'):
    '''
    顶层字段
    text. 必须字段。支持 inline md 的文本内容。
    markdown. 可选字段。用于控制 text 是否解析为 markdown。默认为 true
    channel.可选字段。如果有这个字段，消息会发送到指定讨论组。如果没有，消息会发送到创建机器人时默认的讨论组。
    attachments. 可选。一系列附件

    attachments:
    at_title. 可选。
    at_text. 可选。
    at_color. 可选。用于控制 attachment 在排版时左侧的竖线分隔符颜色
    title和text字段必须有一个。其他的随意组合。
    '''
    params_dict = ({"text": text, "channel": channel, "margdown": margdown, "attachments": [{"title": at_title, "text":
      at_text, "color":at_color}]})
    params = json.JSONEncoder().encode(params_dict)
    #return by_json_send_message(params)

def sen_message(title, text):
    params_str = ({"text": "", "attachments": [{"title": title, "text": text, "color":"#ffa500"}]})
    params = json.JSONEncoder().encode(params_str)
    return by_json_send_message(params)

def by_json_send_message(params):
    headers = {"Content-Type" : "application/json"}
    bearychat_url = "https://hook.bearychat.com/=bw7by/incoming/c8a733da6e453ec05e5bb2883d8af05e"

    conn = httplib.HTTPConnection("hook.bearychat.com")
    conn.request(method="POST", url=bearychat_url, body=params, headers=headers)
    response = conn.getresponse()
    return response.status

## vim: set ts=2 sw=2: #

if __name__ == '__main__':
  send_message("hello world", "what's this")
  new_send_message("hello text", True, u'广告-数据', "hello at_title", "hello at_text", "#ffa500")
