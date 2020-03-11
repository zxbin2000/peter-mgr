# Peter Scheme Management

## Install

npm install git://github.com/zxbin2000/peter-mgr.git#v2.5.1

## Using Peter

```
文件引入：

let peter = require('peter').getManager('key');
let db_url = require('config').get('schema');

peter.bindDb(db_url, function() { 
  console.log('mongodb connected...') 
});

更新 Schema 操作：

$ npm run monitor
$ schema update schema/db.schema

```

## 版本说明

### v2.5.1 2020-03-11

* Mongodb NodeJS Driver 更新至 v3.2.7 版本；
* 删除 Thenjs 依赖，增加 Promise 支持；
* schema update 操作增加变更判断逻辑，仅在 schema 有变化时，才更新数据库；
* schema 对集合定义使用的 "[", "]" 符号，仅作为逻辑层描述符号，存储层不再体现，方便业务层操作；
* 统一配置文件，约定使用 config 配置中 "schema" 属性来作为数据库链接；
* 统一 manyGet 操作表现，删除 getMany 方法；
* 增加 findUnion 操作，用于按照某一字段去重查询操作；
* 删除 link、unlink、isLinked、getLinks 方法，简化数据链接关系；

## Peter 方法使用说明

### 方法约定

1. 所有非 Promise 异步方法，最后一个参数均为 Callback 函数；
2. Promise 方法约定，原 Peter 方法名后加 “Async” 后缀，即为 Promise 方法；

例如：
```
// 默认 Callback 实现
peter.create('@User', { name: 'demo' }, function(err, userId) { 
  console.log(err, userId); 
});

// 同名 Promise 实现
peter.createAsync('@User', { name: 'demo' }).then(userId => {
  console.log('userId = ', userId);
});
```

### peter.create 向指定 Collection 中插入新文档
