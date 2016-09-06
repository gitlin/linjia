# coding=utf8

import os
import sys
import json
import random
import logging
import traceback
from functools import wraps
from logging import handlers

import torndb
from fabric.api import run as _run, env, sudo, cd

reload(sys)
sys.setdefaultencoding("utf8")

# mysql配置,用于获取配置信息

HOST = "111.11.2211.1"
DB = "BI"
USER = ""
PASSWORED = ""

# fabric配置
ENV_USER = ""

logger = logging.getLogger("deploy")


def log_init():
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    rotate = handlers.RotatingFileHandler("deploy.log", maxBytes=10 * 1024 * 1024, backupCount=5)
    rotate.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s')
    console.setFormatter(fmt)
    rotate.setFormatter(fmt)
    logger.addHandler(console)
    logger.addHandler(rotate)
    logger.setLevel(logging.DEBUG)


log_init()


def log(level):
    """记录日志"""
    assert level in ("debug", "info", "warn", "error")

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            msg = "msg: {0}".format(args if len(args)>1 else args[0])
            if kwargs:
                msg = "{0}, parameters: {1}".format(msg, kwargs)
            getattr(logger, level)(msg)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_error(func):
    """记录错误日志"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            msg = "func: {0}, parameters:{1}, {2}".format(func.__name__, args, kwargs)
            logger.error(msg)
            logger.error(traceback.format_exc())
            raise

    return wrapper


def run(cmd):
    """fabric执行远程命令"""
    logger.debug("cmd: %s\n", cmd)
    if ENV_USER != "root":
        sudo(cmd)
    else:
        _run(cmd)


class Connection(object):
    def __init__(self, db=None):
        if db is None:
            self.conn = torndb.Connection(HOST, DB, USER, PASSWORED)
        else:
            self.conn = torndb.Connection(db["inner"], db["db"], db["user"], db["password"])

    def __getattr__(self, name):
        func = getattr(self.conn, name)
        return log("info")(func)


class ObjectDict(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value


class AutoDeploy(object):
    def __init__(self):
        self.conn = Connection()


# 增加定时任务
def add_crontab(cmd):
    run(u"echo '{0}'  >> /var/spool/cron/root".format(cmd))


_config_tpl = """\
[store]
host = {store_inner}
user = {store_user}
password = {store_password}
db = {store_db}

[analyse]
host = {analyse_inner}
user = {analyse_user}
password = {analyse_password}
db = {analyse_db}
"""

_report_tpl = """\
export LANG="en_US.UTF-8"
work_home={work_home}
data_home=${{work_home}}/bidata-rsync
clean_home=${{work_home}}/clean
shell_home=${{work_home}}/shell
backup_home="${{work_home}}/backup"
day=`date +'%Y-%m-%d'`
hour=`date +'%H%M'`
time=`date +'%H_%M'`
mkdir ${{backup_home}}/${{day}}
mkdir -p {log_path}/${{day}}

ls ${{data_home}} > ${{shell_home}}/list/list_${{day}}_${{hour}}
name=${{clean_home}}/${{day}}_${{time}}_$RANDOM.log

for i in `cat ${{shell_home}}/list/list_${{day}}_${{hour}}`
do
    cat ${{data_home}}/$i >> $name
    mv ${{data_home}}/$i ${{backup_home}}/${{day}}
done

/root/anaconda/bin/python {shell} $name  >>  {log_path}/${{day}}/${{hour}}.log  2>&1

"""


_clean_tpl = """\
#!/bin/bash
export LANG="en_US.UTF-8"
work_home={work_home}
clean_home="${{work_home}}/clean"
target_day=`date -d "-3 day" +%F`

rm -f ${{clean_home}}/${{target_day}}*

if [ $? -eq 0 ];then
echo  "The data in the path of ${{1}}  has been cleaned !"
else
echo  "The action of cleaning dirty data in ${{1}} was failed !"
fi
"""


_extend_rsync_tpl = """\
#!/bin/bash
export LANG="en_US.UTF-8"
work_home={work_home}
work_dir={work_dir}
data_home=${{work_home}}/xiaohao
clean_home=${{work_home}}/clean
shell_home=${{work_home}}/shell
backup_home="${{work_home}}/backup"

day=`date +"'%Y-%m-%d'"`
log="${{work_dir}}/log/crontab/$day"
script="${{work_dir}}/shell/analysis/extract_extend.py"

mkdir -p $log
hour=`date +"'%H%M'"`
mkdir -p  ${{backup_home}}/${{day}}
lists=$shell_home/list/list_${{day}}_${{hour}}
ls ${{data_home}} > $lists


/root/anaconda/bin/python $script ${{data_home}} ${{backup_home}}/${{day}} $lists  >> $log/${{day}}_${{hour}}.log  2>&1

"""


_secure_rsync_tpl = """\
#!/bin/bash

work_home={work_home}
echo `date`

count=`ps -ef |grep ${{work_home}}/shell/extend_rsync.sh |grep -v "grep" |wc -l`
if [ 0 == $count ];then
    echo "start"
    /bin/bash ${{work_home}}/shell/extend_rsync.sh
    echo -n  "end "
    echo  `date`
else
    echo "process already exist"
fi

echo '-------------------------------'

"""

_target_snapshot_tpl = """\
#!/bin/bash
work_home={work_home}
dir=${{work_home}}/kuaizhao
target_day=`date -d "'-2 day'" +%F`


#target_day=${{1}}

/usr/bin/pigz ${{dir}}/${{target_day}}/*/*
"""

_target_consume_tpl = """\
#!/bin/bash
work_home={work_home}
dir=${{work_home}}/backup
target_day=`date -d "'-2 day'" +%F`


#target_day=${{1}}

ls ${{dir}}/${{target_day}} > ${{work_home}}/tmp_list

for i in `cat ${{work_home}}/tmp_list`
do
/usr/bin/pigz ${{dir}}/${{target_day}}/$i
done
"""


_rsync_tpl = """
[{rsync_name}]
path = {rsync_path}
comment = {comment}
auth users = longtu
secrets file = /etc/rsyncd.secrets
"""


class Application(AutoDeploy):
    """app应用程序"""
    def __init__(self, app_id):
        super(Application, self).__init__()
        self.app_id = app_id
        self.app = self.get_app()
        self.attr = self.get_attr()
        self.device = self.get_device()
        self.work_dir = os.path.join(os.path.dirname(self.attr.shell), "games", self.app.node)
        self.set_up()

    def get_app(self):
        sql = "select * from manage_ana_app where id={0}".format(self.app_id)
        res = self.conn.get(sql)
        return ObjectDict(res)

    def get_attr(self):
        sql = "select * from manage_ana_attr where id={0}".format(self.app.device_id)
        res = self.conn.get(sql)
        return ObjectDict(res)

    def get_device(self):
        sql = "select * from manage_device where id={0}".format(self.attr.equipment_id)
        res = self.conn.get(sql)
        return ObjectDict(res)

    def get_analyse_db(self):
        sql = "select * from kv where `key`='analyse'"
        res = self.conn.get(sql)
        device = json.loads(res["value"])["device"]
        sql = "select d.inner, a.user, a.password, a.db from manage_device as d join manage_sql_attr as a \
               on d.id=a.equipment_id  where d.id={0}".format(device)
        res = self.conn.get(sql)
        return res

    def get_store_db(self):
        sql = "select d.inner, a.user, a.password, a.db from manage_device as d join manage_sql_attr as a \
               on d.id=a.equipment_id  where d.id={0}".format(self.app.store)
        res = self.conn.get(sql)
        return res

    def set_up(self):
        env.user = ENV_USER
        env.host_string = self.device.inner

    def clear(self):
        """清档操作"""
        logger.info("clear action")
        script = os.path.join(self.work_dir, "shell", "analysis", "manage.py")
        cmd = "/root/anaconda/bin/python {0} clear ensure:no".format(script)
        run(cmd)

    def deploy(self):
        """部署app"""
        self.deploy_analyse()
        self.deploy_rsync()

    def deploy_snapshot(self):
        self.deploy_analyse_snapshot()
        self.deploy_rsync_snapshot()

    def deploy_consume(self):
        self.deploy_analyse_consume()
        self.deploy_rsync_consume()

    def deploy_rsync(self):
        """配置rsync"""
        logger.info("deploy rsync")
        work_home = os.path.join(self.app.rsync_path, self.app.node)
        log_path = os.path.join(self.work_dir, "log", "crontab")
        shell = os.path.join(self.work_dir, "shell", "analysis", "extract.py")
        run("mkdir -p {0}".format(work_home))
        with cd(work_home):
            run("mkdir -p backup clean shell/list bidata-rsync log")
            run("chmod 777 bidata-rsync")
            run("chown nobody:nobody bidata-rsync")
            report_buf = _report_tpl.format(**locals())
            clean_buf = _clean_tpl.format(**locals())
            report_script, clean_script = "{0}.sh".format(self.app.node), "cleandata.sh"

            report_shell = os.path.join(work_home, "shell", report_script)
            clean_shell = os.path.join(work_home, "shell", clean_script)

            run("echo '{0}' > {1}".format(report_buf, report_shell))
            run("echo '{0}' > {1}".format(clean_buf, clean_shell))
            run("chmod +x {0} {1}".format(report_shell, clean_shell))

            log_path = os.path.join(work_home, "log")

            add_crontab("#*/5 *  * * * {0}  >> {1}/report.log 2>&1".format(report_shell, log_path))
            add_crontab("""#25 10 * * *  {0}  "{1}" >> {2}/clean.log 2>&1""".format(clean_shell, work_home, log_path))

        rsync_buf = _rsync_tpl.format(rsync_name=self.app.rsync_name, rsync_path=os.path.join(work_home, "bidata-rsync"), comment=self.app.node)
        sudo("echo '{0}' >> /etc/rsyncd.conf".format(rsync_buf))

    def deploy_rsync_snapshot(self):
        """配置快照rsync"""
        logger.info("deploy rsync consume")
        work_home = os.path.join(self.app.snapshot_path, self.app.node + "-kuaizhao")
        run("mkdir -p {0}".format(work_home))
        with cd(work_home):
            run("mkdir -p kuaizhao shell")
            run("chmod 777 kuaizhao")
            run("chown nobody:nobody kuaizhao")
            target_buf = _target_snapshot_tpl.format(**locals())
            target_script = "target.sh"
            target_shell = os.path.join(work_home, "shell", target_script)

            run("echo '{0}' > {1}".format(target_buf, target_shell))
            run("chmod +x {0}".format(target_shell))
            add_crontab("#0 5 * * * {0}".format(target_shell))

        rsync_name = "_".join([self.app.node, "kuaizhao"])
        rsync_buf = _rsync_tpl.format(rsync_name=rsync_name, rsync_path=os.path.join(work_home, "kuaizhao"), comment=rsync_name)
        sudo("echo '{0}' >> /etc/rsyncd.conf".format(rsync_buf))

    def deploy_rsync_consume(self):
        """配置消耗rsync"""
        logger.info("deploy rsync consume")
        work_home = os.path.join(self.app.consume_path, self.app.node + "-xiaohao")
        work_dir = self.work_dir + "-xiaohao"
        run("mkdir -p {0}".format(work_home))

        with cd(work_home):
            run("mkdir -p backup shell/list xiaohao")
            run("chmod 777 xiaohao")
            run("chown nobody:nobody xiaohao")
            extend_rsync_buf = _extend_rsync_tpl.format(**locals())
            secure_rsync_buf = _secure_rsync_tpl.format(**locals())
            target_buf = _target_consume_tpl.format(**locals())

            extend_rsync_script, secure_rsync_script, target_script = "extend_rsync.sh", "secure_rsync.sh", "target.sh"

            extend_rsync_shell = os.path.join(work_home, "shell", extend_rsync_script)
            secure_rsync_shell = os.path.join(work_home, "shell", secure_rsync_script)
            target_shell = os.path.join(work_home, "shell", target_script)

            run("echo '{0}' > {1}".format(extend_rsync_buf, extend_rsync_shell))
            run("echo '{0}' > {1}".format(secure_rsync_buf, secure_rsync_shell))
            run("echo '{0}' > {1}".format(target_buf, target_shell))
            run("chmod +x {0} {1} {2}".format(extend_rsync_shell, secure_rsync_shell, target_shell))

            add_crontab(u"\n\n#{0} 消耗数据".format(self.app.game))
            add_crontab("#*/5 * * * * {0}  >> {1}/shell/s.log 2>&1".format(secure_rsync_shell, work_home))
            add_crontab("#0 6 * * * {0}".format(target_shell))

        rsync_name = "_".join([self.app.node, "xiaohao"])
        rsync_buf = _rsync_tpl.format(rsync_name=rsync_name, rsync_path=os.path.join(work_home, "xiaohao"), comment=rsync_name)
        sudo("echo '{0}' >> /etc/rsyncd.conf".format(rsync_buf))

    def deploy_analyse(self):
        """部署分析脚本"""
        logger.info("deploy analyse")
        run("mkdir -p {0}".format(self.work_dir))
        with cd(self.work_dir):
            run("mkdir -p clean data history log/crontab shell/analysis")
            shell_dir = os.path.join(self.work_dir, "shell", "analysis")
            log_dir = os.path.join(self.work_dir, "log")
            with cd("shell/analysis"):
                # 不变文件
                immu_files = ("bi", "main.py", "manage.py", "ana.py", "ana_pre.py", "cron.py",
                              "ana_multi_roleid.py", "ana_multi_roleid_pre.py",
                              "hour.py", "mon.py", "rename.py", "clean.py")
                for f in immu_files:
                    run("ln -s ../../../../shell/analysis/{0} {0}".format(f))

                # 可变文件
                mu_files = ("extract.py", "config.ini", "ex.py")
                for f in mu_files:
                    run("cp ../../../../shell/analysis/{0} {0}".format(f))

                # 修改可变文件
                if self.app.version in ["new"]:
                    run("sed -i 's/-t.old/-t new/' extract.py")
                    run("sed -i 's/-t.old/-t new/' ex.py")

                analyse = self.get_analyse_db()
                store = self.get_store_db()
                values = {"{0}_{1}".format("analyse", k): v for k, v in analyse.iteritems()}
                values.update({"{0}_{1}".format("store", k): v for k, v in store.iteritems()})
                config_buf = _config_tpl.format(**values)
                run("echo '{0}' > config.ini".format(config_buf))

                ana_script = "ana_pre.py"
                if self.app.multi_role:
                    ana_script = "ana_multi_roleid_pre.py"
                add_crontab(u"\n\n#{0}".format(self.app.game))
                minute, hour = random.randint(1, 60), random.randint(2, 6),
                add_crontab("#{0} {1} * * * /root/anaconda/bin/python {2}/{3}".format(minute, hour, shell_dir, ana_script))
                hour_minute = random.randint(0, 6)
                add_crontab("#{0} 9,12,16,18,20,23 * * * /root/anaconda/bin/python {1}/hour.py >> {2}/hour.log 2>&1".format(hour_minute, shell_dir, log_dir))
                hour_mon = random.randint(13, 17)
                add_crontab("#{0} {1} 1 * * /root/anaconda/bin/python {2}/mon.py >> {3}/month.log 2>&1".format(minute, hour_mon, shell_dir, log_dir))
                hour_minute = random.randint(19, 20)
                add_crontab("#{0} {1} 1 * * /root/anaconda/bin/python {2}/clean.py >> {3}/clean.log 2>&1".format(minute, hour_minute, shell_dir, log_dir))

    def deploy_analyse_snapshot(self):
        """部署快照分析脚本"""
        logger.info("deploy analyse snapshot")
        work_dir = self.work_dir + "-kuaizhao"
        run("mkdir -p {0}".format(work_dir))
        with cd(work_dir):
            run("mkdir -p clean data history log/crontab shell/analysis")
            shell_dir = os.path.join(work_dir, "shell", "analysis")
            # log_dir = os.path.join(work_dir, "log")
            with cd("shell/analysis"):
                # 不变文件
                immu_files = ("bi", "main.py", "manage.py")
                for f in immu_files:
                    run("ln -s ../../../../shell/analysis/{0}".format(f))

                # 可变文件
                mu_files = ("config.ini", "snapshot.py", "snapshot_pre.py")
                for f in mu_files:
                    run("cp ../../../../shell/analysis/{0} {0}".format(f))

                snapshot_dir = "/".join([self.app.snapshot_path, self.app.node + "-kuaizhao", "kuaizhao"])
                run("sed -i 's#/data2/jianmo/kuaizhao#%s#g' snapshot.py" % snapshot_dir)
                run("sed -i 's#/data2/jianmo/kuaizhao#%s#g' snapshot_pre.py" % snapshot_dir)

                analyse = self.get_analyse_db()
                store = self.get_store_db()
                values = {"{0}_{1}".format("analyse", k): v for k, v in analyse.iteritems()}
                values.update({"{0}_{1}".format("store", k): v for k, v in store.iteritems()})
                config_buf = _config_tpl.format(**values)
                run("echo '{0}' > config.ini".format(config_buf))

                add_crontab(u"\n\n#{0} 快照".format(self.app.game))
                add_crontab("#0 4 * * * /root/anaconda/bin/python {0}/snapshot_pre.py".format(shell_dir))

    def deploy_analyse_consume(self):
        """部署消耗分析脚本"""
        logger.info("deploy analyse")
        work_dir = self.work_dir + "-xiaohao"
        run("mkdir -p {0}".format(work_dir))
        with cd(work_dir):
            run("mkdir -p clean data history log/crontab shell/analysis")
            shell_dir = os.path.join(work_dir, "shell", "analysis")
            # log_dir = os.path.join(work_dir, "log")
            with cd("shell/analysis"):
                # 不变文件
                immu_files = ("bi", "main.py", "manage.py", "ana_extend.py", "ana_extend_pre.py", "tar_consume.py")
                for f in immu_files:
                    run("ln -s ../../../../shell/analysis/{0} {0}".format(f))

                # 可变文件
                mu_files = ("config.ini", "extract_extend.py")
                for f in mu_files:
                    run("cp ../../../../shell/analysis/{0} {0}".format(f))

                # 修改可变文件
                if self.app.version in ["new"]:
                    run("sed -i 's/-t.old/-t new/' extract_extend.py")

                analyse = self.get_analyse_db()
                store = self.get_store_db()
                values = {"{0}_{1}".format("analyse", k): v for k, v in analyse.iteritems()}
                values.update({"{0}_{1}".format("store", k): v for k, v in store.iteritems()})
                config_buf = _config_tpl.format(**values)
                run("echo '{0}' > config.ini".format(config_buf))

                add_crontab(u"\n\n#{0} 消耗分析".format(self.app.game))
                add_crontab("#0 4 * * * /root/anaconda/bin/python {0}/ana_extend_pre.py".format(shell_dir))
                add_crontab("#0 19 * * * /root/anaconda/bin/python {0}/tar_consume.py".format(shell_dir))


def deploy_ana(app_id):
    """部署分析脚本"""
    logger.info("deploy ana, app_id:%s\n", app_id)
    app = Application(app_id)
    app.deploy_analyse()


def deploy_rsync(app_id):
    """部署分析脚本"""
    logger.info("deploy rsync, app_id:%s\n", app_id)
    app = Application(app_id)
    app.deploy_rsync()


def deploy_app(app_id):
    """部署app"""
    logger.info("deploy app, app_id:%s\n", app_id)
    app = Application(app_id)
    app.deploy()


def deploy_snapshot(app_id):
    """部署快照"""
    logger.info("deploy snapshot, app_id:%s\n", app_id)
    app = Application(app_id)
    app.deploy_snapshot()


def deploy_consume(app_id):
    """部署消耗"""
    logger.info("deploy consume, app_id:%s\n", app_id)
    app = Application(app_id)
    app.deploy_consume()


def clear_app(app_id):
    """清档"""
    logger.info("clear app, app_id:%s\n", app_id)
    app = Application(app_id)
    app.clear()


def test(app_id):
    print("test", app_id)


if __name__ == "__main__":
    test()
