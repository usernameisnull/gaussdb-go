## 基础的协议代码的来源
最开始瞄准openGauss主要的底层协议的代码来源于: https://gitee.com/opengauss/openGauss-connector-go-pq 这个仓库  
这个仓库的提交记录: 3d247719, 提交人: moseszane168(张三喜)  

## GaussDB的协议不同
因为协议的不同: 跑UT的时候, 连接GaussDB的主备数据库, 导致主备切换    
原因: gaussdb要求BE后面跟S  
- AuthenticationMD5Password (B)


## PG协议的文档
PG文档里的: 
- [Chapter 55. Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- 中文文档: https://postgresql.ac.cn/docs/current/protocol.html
### 怎么看协议
- [消息格式](https://postgresql.ac.cn/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS)
- PBDES, 对于GaussDB这得是一组
```txt
Parse (F) --这里的F表示是Frontend的消息
    Byte1('P') --这才是消息的类型
        标识此消息为 Parse 命令。
    Int32
        消息内容（包括自身）的字节长度。
    String
        目标预处理语句的名称（空字符串选择未命名的预处理语句）。
    String
        要解析的查询字符串。
    Int16
        指定参数数据类型的数量（可能为零）。请注意，这并不表示查询字符串中可能出现的参数数量，仅仅表示前端希望预先指定类型的参数数量。
        然后，对于每个参数，都有以下内容：
    Int32
        指定参数数据类型的对象 ID。在此处放置零等同于不指定类型。
```
---
```txt
Bind (F)
    Byte1('B')
        标识此消息为 Bind 命令。
    Int32
        消息内容（包括自身）的字节长度。
    String
        目标 portal 的名称（空字符串选择未命名的 portal）。
    String
        源预处理语句的名称（空字符串选择未命名的预处理语句）。
    Int16
        后面参数格式代码的数量（在下面用 C 表示）。这可以是零，表示没有参数或所有参数都使用默认格式（文本）；或者为一，在这种情况下，指定的格式代码应用于所有参数；或者可以等于实际参数的数量。
    Int16[C]
        参数格式代码。每个代码目前必须是零（文本）或一（二进制）。
    Int16
        后面参数值的数量（可能为零）。这必须与查询所需的参数数量匹配。
        接下来，对于每个参数，都会出现以下字段对：
    Int32
        参数值的长度（以字节为单位）（此计数不包括自身）。可以是零。作为特殊情况，-1 表示 NULL 参数值。在 NULL 的情况下，后面没有值字节。
    Byten
        参数的值，格式由关联的格式代码指示。n 是上面的长度。
        在最后一个参数之后，会出现以下字段：
    Int16
        后面结果列格式代码的数量（在下面用 R 表示）。这可以是零，表示没有结果列或所有结果列都应使用默认格式（文本）；或者为一，在这种情况下，指定的格式代码应用于所有结果列（如果有）；或者可以等于查询的实际结果列数量。
    Int16[R]
        结果列格式代码。每个代码目前必须是零（文本）或一（二进制）。
```
---
```txt
RowDescription (B) --这里的F表示是Backend的消息
    Byte1('T')
        标识此消息为行描述。
    Int32
        消息内容（包括自身）的字节长度。
    Int16
        指定一行中的字段数量（可能为零）。
        然后，对于每个字段，都有以下内容：
    String
        字段名称。
    Int32
        如果字段可以被识别为特定表的列，则为该表的对象 ID；否则为零。
    Int16
        如果字段可以被识别为特定表的列，则为该列的属性编号；否则为零。
    Int32
        字段数据类型的对象 ID。
    Int16
        类型大小（请参阅 pg_type.typlen）。请注意，负值表示可变宽度类型。
    Int32
        类型修饰符（请参阅 pg_attribute.atttypmod）。修饰符的含义是特定于类型的。
    Int16
        正在用于该字段的格式代码。目前为零（文本）或一（二进制）。在 Describe 的语句变体返回的 RowDescription 中，格式代码尚不确定，将始终为零。

```
---
```txt
ErrorResponse (B)
    Byte1('E')
        标识此消息为错误。
    Int32
        消息内容（包括自身）的字节长度。
        消息体由一个或多个标识字段组成，后跟一个零字节作为终止符。字段可以按任意顺序出现。对于每个字段，都有以下内容：
    Byte1
        一个标识字段类型的代码；如果为零，则这是消息终止符，并且后面没有字符串。当前定义的字段类型列在 第 54.8 节中。由于将来的版本可能会添加更多字段类型，因此前端应静默忽略未知类型的字段。
    String
        字段的值。
        
Execute (F)
    Byte1('E')
        标识此消息为 Execute 命令。
    Int32
        消息内容（包括自身）的字节长度。
    String
        要执行的 portal 的名称（空字符串选择未命名的 portal）。
    Int32
        如果 portal 包含一个返回行的查询，则返回的最大行数（否则忽略）。零表示“无限制”。

```
---
```txt
ParameterStatus (B)
    Byte1('S')
        标识此消息为运行时参数状态报告。
    Int32
        消息内容（包括自身）的字节长度。
    String
        正在报告的运行时参数的名称。
    String
        参数的当前值。
```
---
```txt
Sync (F)
    Byte1('S')
        标识此消息为 Sync 命令。
    Int32(4)
        消息内容（包括自身）的字节长度。
```