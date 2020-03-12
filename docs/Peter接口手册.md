# Peter 使用手册

## peter.create 向指定 Collection 插入新文档

* 参数：name ｜ json
* 返回：peter_id
* 示例：peter.create('@User', { name: 'demo' }, callback);

## peter.createS 向指定 Collection 插入新文档 - 自增 ID

* 参数：name | json
* 返回：serial_id
* 示例：peter.createS('@User', { name: 'demo' }, callback);

## peter.destroy 删除指定文档记录

* 参数：peter_id
* 示例：peter.destroy('5d4a785000000613c14961b1', callback);

## peter.get 读取指定文档记录

* 参数：peter_id | fields[ optional ]
* 示例：peter.get('5d4a785000000613c14961b1', ['name', 'age'], callback);

## peter.manyGet 批量读取指定文档记录

* 参数：peter_id 数组 | fields[ optional ]
* 返回：以 peter_id 为键名的 Map 对象，例：{ '5d4a785000000613c14961b1': { name: 'demo', age: 10 } }
* 示例：peter.manyGet(['5d4a785000000613c14961b1'], ['name', 'age'], callback);

## peter.set 更新指定文档记录

* 参数：peter_id | json | options
* 示例：peter.set('5d4a785000000613c14961b1', { name: 'demo2', age: 11 }, callback);

## peter.replace 替换指定文档的某一字段值

* 说明：当文档中存在这个字段时，才能进行替换操作。
* 参数：peter_id | field name | field value
* 示例：peter.replace('5d4a785000000613c14961b1', 'name', 'demo3', callback);

## peter.insert 向指定文档插入新字段

* 说明：当文档中不存在这个字段时，才能进行插入操作。
* 参数：peter_id | field name | field value
* 示例：peter.insert('5d4a785000000613c14961b1', 'gender', 1, callback);

## peter.remove 删除文档记录的指定字段

* 参数：peter_id | field or field array
* 示例：peter.remove('5d4a785000000613c14961b1', ['age', 'gender'], callback);

## peter.find 查询给定条件的文档记录

* 参数：coll_name | query | options
* 示例：peter.find('@User', { age: { $gte: 10 } }, { limit: 20 }, callback);
* 说明：query 查询条件可以为 {}, 但不能为 null; options 可以使用 skip、limit、sort 等选择项

## peter.findOne 查询给定条件的一个文档记录

* 参数：coll_name | query | options
* 示例：peter.findOne('@User', { age: { $gte: 10 } }, { sort: { name: 1 } }, callback);

## peter.findUnion 按照某一字段进行去重查询

* 参数：coll_name | field | query | options
* 示例：peter.findUnion('@Comment', 'news_id', { posted_by: '5d4a785000000613c14961b1' }, { limit: 20 }, callback);

## peter.findOneAndUpdate 查询并更新指定条件的一个文档

## peter.findOneAndReplace 查询并替换指定条件的一个文档

## peter.findOneAndDelete 查询并删除指定条件的一个文档

## peter.push 向集合中加入新元素

## peter.pop 集合中弹出最顶层元素

## peter.replaceElementByKey

## peter.replaceElement

## peter.replaceElementByIndex

## peter.removeElementByKey

## peter.removeElement

## peter.removeElementsByCond

## peter.getElementByKey

## peter.getElementByIndex

## peter.getElementsByRange

## peter.aggregate 聚合查询操作

## peter.count 按照查询条件计数

## peter.distinct 获取某一字段的唯一值

## peter.increase 字段自增长操作