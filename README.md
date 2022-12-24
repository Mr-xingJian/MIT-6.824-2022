# MIT-6.824-2022
MIT-6.824分布式lab代码.


## 关于分布式
不知道什么时候开始，分布式这三个字就走进了我的视野。分布式像一座高山，远远的矗立，令人望而生畏，我下了无数次的决心想要去攀登，可是每次走到山脚就放弃了。所以一直以来我对于分布式都是浅尝辄止，看过一些粗浅的文章，却从没见过其真面目。

而这一次，我下定了决心要开始登山，说小一点是为了以后面试被问到分布式时不再手忙脚乱，说大一点则是既然选择了技术这条路，那么总要看看高处的风景吧。因为6.824课程早已是名声在外，所以选择跟着这门课程来学习分布式系统。


## MapReduce
MapReduce还是相对简单的，基本就是一个任务分发然后收集结果的过程，核心其实就是如何保证所有的任务都要被执行到，并且只有所有Map操作完成之后才能开始进行Reduce。

## Raft
### 2A Leader Election
Done
### 2B Log Replication
Done
### 2C Persistence
Done

2C这个Lab出现了一个比较有趣的bug，最后一个Test(TestUnreliableChurn2C)总是会偶然性的失败。测试失败有一个规律，单独测试这个case的时候不会失败，但是如果使用脚本来进行多次测试就会出现偶然性的失败，例如测试10次就会有1次失败。我这里加了很多日志来进行排查，最后也只是发现情况都是因为长时间没有对一个值达成一致，另外发现有很多地方网络出现了非常离谱的延延时，有的2s后rpc请求才到，导致长时间无法选出leader，也没法进行共识流程。

因为本身平时也在工作，只能下班之后继续查一下这个问题，最开始怀疑是不是因为go的time.Sleep不靠谱导致ticker不正常，然后也可能是锁竞争太频繁导致出现延时，最后甚至怀疑是不是提供的网络模拟组件有问题，经过各种尝试发现都不是。这也让我比较苦恼，因为一旦进行批量测试就会有失败，然而单测又测不出什么问题。

后来看到Raft.killed这个方法注释的一句话"the issue is that long-running goroutines use memory and may chew up CPU time, perhaps causing later tests to fail and generating confusing debug output. any goroutine with a long-running loop should call killed() to check whether it should stop."，突然又联想到之前测试的时候确实会出现一些cpu飙高的情况，有一次测试甚至让我的云服务器直接cpu拉满，重启才得以解决，TestUnreliableChurn2C这个case就是启动3个client去执行很多Start请求，这一步可能导致已经kill掉的Raft实例执行后续流程，这时候整个流程是走不通的，网络模拟组件对于已经kill掉的Raft请求也会进行长时间的延时，系统goroutine数量也可能暴增，导致后续流程出现延时。

想到这里有种顿悟的感觉，于是赶紧在一些流程中加上了Raft.killed()的判断，然后进行一下批量测试，立马就通过了，这时候才明白原来真理就在其中。

当然，这里仅仅是通过了测试，真实的问题可能我也没有搞清楚，还要继续排查，这个就是后面的工作了。