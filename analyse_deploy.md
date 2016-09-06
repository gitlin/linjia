## 分析脚本自动化介绍

文件: dataeye/script/fabfile

作用: 部署分析脚本，rsync，清档等

后台界面配置如下:

![](img/analyse_deploy.png)


后台界面已提供游戏全部署，清档操作

但fabfile.py分析脚本，rsync可以独立部署，经常出现rsync已经部署好，只需要部署分析脚本就好

app_id就是上图的右角标，是每个游戏的主键id

- 查看可用命令

    cd /data/biweb/live_production/script

    fab --list

- 只部署分析脚本

    fab deploy_ana:app_id

- 只部署rsync

    fab deploy_rsync:app_id

- 部署分析脚本和rsync

    fab deploy_app:app_id

- 清档

    清档之前请确认gameid是否正常，gameid不对会删除其它游戏的数据，如不对请登陆后台删除history下对应的gameid

    fab clear_app:app_id


###注意:
1. rsync路径只写根目录就可以了，脚本会自动加入节点名字
2. 加入crontab时会添加注释，若要开启清手工去除#注释



