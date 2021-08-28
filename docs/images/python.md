##### 文件 I/O

+ 输入输出

  ```python
  # normal
  print("string")
  
  # multi string, commas are replaced with blankspaces
  print("string1", "string2", "string3")
  # >> string1 string2 string3
  ```

  可以直接输出非字符串的内容例如数字，可以输出算数表达式的结果

  ```python
  print(100+200)
  # >> 300
  ```

  格式化字符串的用法

  ```python
  str = "fstr"
  print(f'string with format is {str}')
  # >> string with format is fstr
  ```

  输入使用 ```input``` 函数

  ```python
  str = input("请输入：")

+ 文件操作

  打开文件

  ```python
  file object = open(file_name [, access_mode][, buffering])
  ```

  常用的打开文件模式的参数，都是字符串的形式

  | Mode | Description                                           |
  | ---- | ----------------------------------------------------- |
  | x    | 写，新建文件                                          |
  | b    | 二进制模式                                            |
  | +    | 打开文件进行更新，可读可写                            |
  | r    | 只读，cursor 在开头                                   |
  | w    | 写入，cursor 在开头，会删除原有内容，文件不存在则创建 |
  | a    | 写入，追加到文件末尾                                  |

  关闭文件

  ```python
  file.close()
  ```

  文件读写操作示例

  ```python
  # 打开一个文件
  fo = open("foo.txt", "w")
  fo.write( "www.runoob.com!\nVery good site!\n")
   
  # 关闭打开的文件
  fo.close()
  
  # 打开一个文件，read 参数空缺默认读取整个文件，readline 读取一行
  fo = open("foo.txt", "r+")
  str = fo.read(10)
  print "读取的字符串是 : ", str
  
  # 关闭打开的文件
  fo.close()
  ```

  文件定位

  ```python
  # 查看当前 cursor 位置
  file.tell()
  
  # 改变 cursor 位置
  file.seek(offset [, from])
  ```

+ Excel 文件

  需要引入 pandas 包

  ```python
  import pandas as pd
  
  #read_excel()用来读取excel文件，记得加文件后缀，可以指定 header 起始的行号，header=None 表示不设表头，用 0，1 代替
  data = pd.read_excel('C:/tmp/002/People.xlsx' [, header=x]) 
  print('显示表格的属性:',data.shape)   #打印显示表格的属性，几行几列
  print('显示表格的列名:',data.columns) #打印显示表格有哪些列名
  #head() 默认显示前5行，可在括号内填写要显示的条数
  print('显示表格前三行:',data.head(1)) 
  print('--------------------------华丽的分割线----------------------------')
  #tail() 默认显示后5行，可在括号内填写要显示的条数
  print('显示表格后五行:',data.tail())
  ```

  保存表格之后会出现额外的索引列，可以手动去除

  ```python
  import pandas as pd
  
  rdexcle = pd.read_excel('F:/Practice/py/practise_01.xlsx',header=None)
  rdexcle.columns=['ID','NAME']#设置表头
  rdexcle = rdexcle.set_index('ID',inplace=True) # 设置id为索引, 生成一个新的dataframe，用rdexcel继续引用这个新的。inplace=True 表示在当前表上修改。不用再新建表
  print(rdexcle.columns)
  rdexcle.to_excel('F:/Practice/py/practise_01_out.xlsx')
  print('Done!')
  ```

  

