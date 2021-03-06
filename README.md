# Random Sample over Join Revisit

1. 论文选择：Random Sampling over Joins Revisited

2. 任务定义：自然连接是数据库操作中很重要的一种操作。自然连接是关系R和S在所有公共属性上的等接。但在得到的结果中公共属性只保留一次，其余删除。这篇论文关注数据库中多个表的自然连接问题。在数据库应用中，很多时候需要对多个表进行自然连接，得到结果后再对其某些性质进行研究。然而在大数据环境下，这种操作时间复杂性可能是无法接受的。因此需要研究合理的对该整体结果进行均匀抽样的方法。在原论文中，作者关注了chain join，star join和acyclic join三种join方式，在本实验中由于时间关系，我们只关注chain join的均匀抽样方法。
