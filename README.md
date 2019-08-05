# Peter Scheme Management

## Install

npm install git://github.com/zxbin2000/peter-mgr.git#v2.5.0

## Using Peter

var peterMgr = require('peter').getManager('key');

# Peter Language Grammar

FILE ::= [ CODE | RULE ]+

CODE ::= {{ MULTILINE }}

RULE ::= :: LINE
        [ ** LINE ]+
        CHAINS

CHAINS ::= [ CHAIN ]+
           [ FINAL ]
            ;;

CHAIN ::= [ SUCC | RETOK ]
          [ FAIL | RETERR ]

SUCC ::= => [ {{ EMBED }} ] [ LINE ]

RETOK ::= <= [ {{ EMBED }} ] LINE

RETERR ::= <- [ {{ EMBED }} ] LINE

FAIL ::= -> {{ EMBED }}

FINAL ::= <-- [ {{ EMBED }} ] LINE

EMBED ::= [ LINE ] | [ LINE CHAINS ]

# Release Notes

## 2016-08-19 v1.1.1
* 增加 count 函数，参数：collName, condition，返回符合条件的记录条数

## 2016-09-30 v1.1.2
* 增加定时启动 pp 的方法，启动命令格式如下，定时启动参数使用 cron 表达式
```
dodo -t "*/2, *, *, *, *" scripts.pp
```

### 2016-10-20 v1.1.3
* 增加 findAndModify 函数，参数：collName, filter, update 返回需要修改的记录

### 2016-10-20 v1.1.4
* 增加 findAndModify 兼容性

### 2017-02-09 v1.1.5
* 增加 findAndModify 兼容性

### 2017-02-15 v1.1.6
* 增加 ##BINARY## 下载文件流功能支持

### 2017-02-20 v1.1.6-02
* 增加跨域请求支持

### 2017-02-23 v1.1.7
* 增加对多路径 target 功能支持
* 增加对 cookie-parser 的支持

### 2017-03-17 v1.1.8
* 合并 new 分支中，对 setcookie 的支持

### 2018-03-09 v1.2.0
* $include 支持添加文件夹
* 调用接口 API 时添加自定义过滤器，支持 before 和 async 两种方式