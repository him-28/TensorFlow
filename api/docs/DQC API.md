## 数据查询中心（AD Data Center）

### 域名:

  adc.hunantv.com



## 1. 订单报告

### 1.1 Version

Request: GET	**/version**

Return a server version

<pre>
{
	"version": "1.0"
}
</pre>

### 1.2 Cubes

Request: GET	**/cubes**

获取数据中心所有Cube列表

<pre>
{
	"name":"ad",
	"label":"ad report",
	"descriptioin": "此cube包含广告系统所有维度数据：PV, Impressions...",
	"category":"ad"
}
</pre>

### 1.3 Cube Model

Request: GET **/cube/\<name>/model**

获取指定Cube的模型

<pre>
{
    "name": "contracts",
    "info": {},
    "label": "Contracts",
    "aggregates": [
        {
            "name": "amount_sum",
            "label": "Amount sum",
            "info": {},
            "function": "sum"
        },
        {
            "name": "record_count",
            "label": "Record count",
            "info": {},
            "function": "count"
        }
    ],

    "measures": [
        {
            "name": "amount",
            "label": "Amount",
            "info": {},
            "aggregates": [ "sum" ]
        }
    ],

    "details": [...],

    "dimensions": [...]
}
</pre>


### 1.4 聚合，浏览

* **/cube/\<name>/aggregate**
* **/cube/\<name>/member/\<dim>**
* **/cube/\<name>/fact**
* **/cube/\<name>/cell**

### 1.5 单元，裁减

示例:
<pre>
date:2004
date:2004,1
date:2004,1|class:5
date:2004,1,1|category:5,10,12|class:5
</pre>

指定范围
<pre>
date:2009-2015
date:2009,1-2015,9
</pre>

无边界，范围指定
<pre>
date:2009,1,1-
date:-2015,9,10
</pre>

裁减数据
<pre>
date:2010;2011
</pre>

### 1.6 聚合

Request: GET **/cube/\<cube>/aggregate**

返回Json格式的聚合结果

参数:

* cut － 单元格申明，示例: cut=date:2014,1|category:2|entity:12345
* drilldown - 维度下钻. 示例: drilldown=date 将会给出日期下一级的每个值. 你可以直接指定级别下钻: dimension:level, 类似: drilldown=date:month. 也可以申明下钻的层级:dimension@hierarchy 类似: drilldown=date@ymd 
* aggregates － 聚合计算，用|分隔，示例: aggregates=amount_sum|discount_avg|count
* measures - 
* page - page number
* pagesize - page size
* order - 排序方式
* split -

Response:

A dictionary with keys:

* summary - 
* cells
* total_cell_count
* aggregates
* cell
* levels

Example for request **/aggregate?drilldown=date&cut=item:a**:

```
{
	"summary": {
		"count": 32,
		"amount_sum": 558430
	},
	"cells": [
		{
		"count": 16,
		"amount_sum": 275420,
		"date.year": 2014
		},
		{
		"count": 16,
		"amount_sum": 283010,
		"date.year": 2015
	],
	"aggregates": [
		"amount_sum",
		"count"
	],
	"total_cell_count": 2,
	"cell": [
		{
		"path": ["a"],
		"type": "point",
		"dimension": "item",
		"level_depth": 1
		}
	],
	"levels": { "date": ["year"] }
}
```

### 1.7 事实表

Request: GET **/cube/\<cube>/facts**

返回指定cube的事实表单元数据

参数:

* cut
* page,pagesize
* order
* format - json(default),csv
* fields - 事实表字段列表，逗号分隔, 默认返回所有字段
* header - 申明给定格式的表头，name 或者 label

### 1.8 单一事实

Request: GET **/cube/\<cube>/fact/\<id>**

返回给定id的事实数据，示例: /fact/1024.

### 1.9 维度成员

Request: GET **/cube/\<cube>/members/\<dimension>**

获取指定cube的维度成员

参数:

* cut
* depth - 返回层级，不指定返回所有
* level
* hierarchy - 返回指定层级
* page,pagesize
* order

示例 **/cube/facts/members/item?depth=1**:

```
{
    "dimension": "item"
    "depth": 1,
    "hierarchy": "default",
    "data": [
        {
            "item.category": "a",
            "item.category_label": "Assets"
        },
        {
            "item.category": "e",
            "item.category_label": "Equity"
        },
        {
            "item.category": "l",
            "item.category_label": "Liabilities"
        }
    ],
}
```

报告
======

### 2.1 订单报告

Request: GET **/cube/\<cube>/campaign/report**

### 2.2 投放报告

Request: GET **/cube/\<cube>/launch/report**

### 2.3 创意报告

Request: GET **/cube/\<cube>/creative/report**

### 2.4 广告位报告

Request: GET **/cube/\<cube>/slot/report**

### 2.5 播放器报告

Request: GET **/cube/\<cube>/board/report**

### 2.6 客户报告

Request: GET **/cube/\<cube>/customer/report**




### 1.1 接口

    /campaign/report/<metrics>
    
### 方法

GET

### 纬度
1. 日期

>字段: Date

>格式: yyyy-mm-dd hh

>层级: 月，周，日，小时

>示例: /campaign/pv?date=2015-09-11|

2. AD 广告

>2.1 投放 ｜ 广告位 | 创意

3. Area 地区

>3.1 省, 市

4. Video 视频

>4.1 频道 | S，A，B | 合集 | 正短片

5. OS 操作系统

>5.1 Android | IOS | Others

6. ReachCurve

>6.1 Date | Imp-s

### 请求参数说明
1. 日期

>字段: Date

>格式: yyyy-mm-dd hh

1. request 为报告类型，订单、投放、创意、广告位、播放器和客户

```javascript
var request = {
      
      // 时间范围（date range）必须有，默认为昨天
      // *点击时间控件，可以选择的时间范围为今天/昨天/上周（按自然周）/自定义（只能选择订单期内的日期）
      dr: {
          start:'y-m-d',    //2015-9-2
          end:'y-m-d',      //2015-9-7
      },
	
	  // 请求类型
      // *列表显示所有订单信息，概览显示订单详情
      // type: '1',                    // * 1. list 2.overview

	  // 查询（搜索）条件
      ids: '50',                 // 订单id，支持精确查询，支持多订单查询（合并分析）
      name: '伊利-大本营',        // 订单名称，支持模糊查询，支持多订单查询（合并分析）

      // 维度参数
      para: {
          // 主维度参数，必须有
		  prid: {
			  date: 1,2,3,4    //时间维度，按天、按周、按月、按小时
			  ad: 1,2,3        //广告维度，投放、广告位、创意
			  region: 1,2      //地域维度，省份、城市
			  content: 1,2,3,4 //内容维度，分级(S/A/B)、频道()、合集、正短片
 			  os: 1,2,3    	   //操作系统维度，IOS、Android、others
			  RF: 1		 //reach and frequency维度，1+、2+、……、10+
 		  }
		  
		  // 次维度参数，根据主维度参数会对次维度的参数进行限制
		  subd: {
			  date: 1    //时间维度，按天、按周、按月、按小时
			  ad: 1      //广告维度，投放、广告位、创意
			  region: 1  //地域维度，省份、城市
			  content: 1 //内容维度，分级(S/A/B)、频道()、合集、正短片
			  os: 1      //操作系统维度，IOS、Android、others
			  RF: 1		 //reach and frequency维度，1+、2+、……、10+
 		  }

      },

  };

p = JSON.stringify(request);
```

2. _type_object=p

通知管理后台，参数p为json对象。



### 返回（JSON格式）


```js
    data: {
        // 需要分三组，时间范围、主维度、次维度；每组都可能返回多条数据
		request: 10000,  //AD-requests（原pv数）
        hit: 8000,       //命中数（原展示数）
		click: 1000,     //点击数
		CTR: 12.5%, 	 //点击率，click/hit
		imp-s: 6000,		 //开始播放数
		imp-e: 5500, 	 //结束播放数
		hit-t: 2000,		 //第三方展示数，只有通过第三方请求的订单才显示
		receive-t: 1000, //第三方执行数，只有通过第三方请求的订单才显示
		ihr: 75%, 		 //投放成功率，imp-s/hit
		uv: {
			1: 4500, 	 //UV（只有开始日期选择为订单开始日期才显示，选择一天也显示）
			2: 3000,
			...			
			10+: 100,
		}
		uv-c: {
			1: 4500, 	 //点击UV
			2: 3000,
			...			
			10+: 100,
		}
    },
    err_code: 200,
    err_msg: "",
    success: "1"
```

 将url地址通过POST上行(获取请求-上行)方式返回数据并解析下行json获取。

### 请求实例

* [请求实例](http://dqc.hunantv.com/json/request/player?)

### 请求返回