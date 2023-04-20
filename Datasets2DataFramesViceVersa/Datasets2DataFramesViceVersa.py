

newDeptDS: org.apache.spark.sql.Dataset[Dept] = [dept_id: int, dept_name: string]


newDeptDS.first()

res27: Dept = Dept(1,Sales)

newDeptDS.toDF.first()

es28: org.apache.spark.sql.Row = [1,Sales]


