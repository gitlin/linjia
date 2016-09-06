# coding=utf8

import os
import sys
import inspect
import datetime
import subprocess
from functools import wraps
from collections import defaultdict
from contextlib import contextmanager


from bi.log import log, init as log_init
from bi.manage import Manage as BIManage
from bi import config
from bi import ddl, util


def run(cmd):
    """执行命令"""
    log.info(cmd)
    msg = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True).stdout.read()
    log.info(msg)


@contextmanager
def cd(path):
    cwd = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(cwd)


class Application(object):
    """一个部署应用对象"""
    def __init__(self):
        self.set_up()

    def set_up(self):
        _ = sys.argv[0].rsplit(os.path.sep, 1)
        if len(_) > 1:   # 当前app目录
            path = _[0]
        else:
            path = os.path.realpath(os.getcwd())

        parent = os.path.dirname(os.path.dirname(path))
        self.work_dir = parent
        self.data = os.path.join(parent, "data")
        self.clean = os.path.join(parent, "clean")
        self.history = os.path.join(parent, "history")
        self.log = os.path.join(parent, "log")
        self.shell = os.path.join(parent, "shell", "analysis")
        self.script = os.path.join(self.shell, "main.py")
        self.config = os.path.join(self.shell, "config.ini")

        config.init(self.config)
        log_init(os.path.join(self.log, "manage.log"))

    def realtive_path(self, *path):
        """获得work_dir绝对目录"""
        return os.path.join(self.work_dir, *path)


class Manage(object):
    def __init__(self):
        self.cmds = {}

    def show_help(self):
        """输出支持命令列表"""
        print("support cmd:\n")
        for cmd, func in self.cmds.items():
            # 第一个参数默认是app对象
            argv = inspect.getargspec(func)[0][1:]
            print("{0}  {1}".format(cmd, tuple(argv)))

    def run(self, argv=sys.argv):
        cmd = argv[1]
        if cmd not in self.cmds:
            if cmd not in ["help", "-l", "-h"]:
                print("cmd: {0} not found".format(cmd))
            self.show_help()
        else:
            func = self.cmds[cmd]
            return self.execute(func, *argv[2:])

    def execute(self, func, *args, **kwargs):
        app = Application()
        log.info("action: %s", func.__name__)
        log.info("current app work_dir: %s", app.work_dir)
        nargs = []
        kwargs = {}
        for arg in args:
            if ":" in arg:
                k, v = arg.split(":")
                kwargs[k] = v
            else:
                nargs.append(arg)
        res = func(app, *nargs, **kwargs)
        log.info("done")
        return res

    def command(self, func):
        self.cmds[func.__name__] = func

        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(func, *args, **kwargs)

        return wrapper


manage = Manage()


def ensure(option, msg):
    if "ensure" not in option or option["ensure"] == "yes":
        issure = raw_input(msg)
        if issure != "yes":
            print("命令已取消")
            sys.exit(0)


@manage.command
def shell(app):
    """嵌入python shell"""
    def make_context():
        return dict(app=app)
    context = make_context()
    banner = ''
    # Try IPython
    try:
        try:
            # 0.10x
            from IPython.shell import IPShellEmbed
            ipshell = IPShellEmbed(banner=banner)
            ipshell(global_ns=dict(), local_ns=context)
        except ImportError:
            # 0.12+
            from IPython import embed
            embed(banner1=banner, user_ns=context)
        return
    except ImportError:
        pass
    # Use basic python shell
    import code
    code.interact(banner, local=context)


def model_timesamp(model):
    """获取model的timestamp列"""
    try:
        timestamp_column = model.TIMESTAMP or filter(lambda x: x.endswith("_time"), model.FIELDS)[0]
        return timestamp_column
    except Exception as e:
        log.error("model_timesamp get error: model: %s, error: %s", model, str(e))

    return None


@manage.command
def clear(app, gameid=None, **kwargs):
    """游戏清档
    1.　删除入库表
    2.  清空histor等目录
    """
    models = [ddl.AllUser, ddl.AllAdvice, ddl.AllPayUser, ddl.PayMent, ddl.Login, ddl.Consume,
              ddl.RoleNew, ddl.RoleLogin, ddl.Levelup, ddl.Online, ddl.Mission]
    if gameid is None:
        games = util.get_gameid_from_history(app.history)
    else:
        games = [gameid]
    ensure(kwargs, "clear games:{0},Please yes/no:".format(games))
    log.info("clear, gameids: %s", games)
    for game in games:
        for model in models:
            try:
                model.drop(game)
            except Exception as e:
                log.error("DDL delete error: %s", e)
    with cd(app.work_dir):
        if gameid:
            run("find clean/* -name {0}_*.csv | xargs rm".format(gameid))
            run("rm -rf history/{0}".format(gameid))
            run("rm -rf history/all_role/{0}".format(gameid))
        else:
            run("rm -rf clean/* history/*")

    # 试图清理消耗数据
    xiaohao = os.path.join(os.path.dirname(app.work_dir), app.work_dir + "-xiaohao")
    if not os.path.exists(xiaohao):
        return
    log.info("clear consume, base dir:%s" % xiaohao)
    with cd(xiaohao):
        if gameid:
            run("find clean/ -name {0}_*.csv | xargs rm".format(gameid))
            run("rm -rf history/{0}".format(gameid))
        else:
            run("rm -rf clean/* history/*")


@manage.command
def clear_history(app, ds, gameid=None, **kwargs):
    """补数据时处理history，将大于ds日期的去除"""
    if gameid is None:
        games = util.get_gameid_from_history(app.history)
    else:
        games = [gameid]
    ensure(kwargs, "clear games:{0} date:{1},Please yes/no:".format(games, ds))
    from bi.unit.login import LoginUnit
    from bi.unit.role import RoleUnit
    from bi.unit.payment import PaymentUnit

    historys = (
        (LoginUnit.HISTORY_LOGIN_CSV, LoginUnit.HISTORY_LOGIN_FIELDS),
        (LoginUnit.HISTORY_MAC_CSV, LoginUnit.HISTORY_MAC_FIELDS),
        (RoleUnit.HISTORY_CSV, RoleUnit.HISTORY_FIELDS),
        (PaymentUnit.HISTORY_CSV, PaymentUnit.HISTORY_FIELDS)
    )
    models = (ddl.AllUser, ddl.AllAdvice, ddl.AllPayUser)
    clear_time = util.timestamp(util.todate(ds))
    log.info("clear_history, gameids: %s, end_ds: %s", games, ds)
    for game in games:
        # 处理csv
        for csv, fields in historys:
            path = os.path.join(app.history, game, csv)
            df = util.read_csv(path, names=fields)
            time_field = fields[-1]
            df2 = df[df[time_field]<clear_time]
            log.info("process csv path: %s, pre: %s, now: %s", path, len(df), len(df2))
            df2.to_csv(path, header=False, index=False, mode='w', encoding='utf8')

        # 处理数据库
        for model in models:
            table = model.table_name(game)
            timestamp_column = model_timesamp(model)
            try:
                sql = "delete from {0} where {1} >= {2}"
                sql = sql.format(table, timestamp_column, clear_time)
                log.info(sql)
                model.execute(sql)
            except Exception as e:
                log.error("DDL delete error: %s, sql: %s", e, sql)


@manage.command
def clear_filter_openid(app, ds, gameid=None, **kwargs):
    """清理测试账号的充值数据"""
    models = [ddl.PayMent]
    if gameid is None:
        games = util.get_gameid_from_history(app.history)
    else:
        games = [gameid]
    game_openids = BIManage.filter_openid(app.history)
    ensure(kwargs, "clear games:{0};ds:{1}, Please yes/no:".format(games, ds))
    ds_date = util.todate(ds)
    clear_time_f = util.timestamp(ds_date)
    clear_time_t = util.date_delta(ds_date)[1]
    log.info("clear filter openid, gameids: %s", games)
    # 清理数据库
    for game in games:
        # 根据游戏id获取测试账号
        filter_openids = game_openids.get(game)
        # gameid 的测试账号为空，不作处理
        if not filter_openids:
            continue
        openids = ",".join(map(lambda x: "'%s'" % x, filter_openids))
        # 手动拼接 sql
        for model in models:
            table = model.table_name(game)
            timestamp_column = model_timesamp(model)
            try:
                sql = "delete from {0} where {1} >= {2} and {1} < {3} and openid in ({4})"
                sql = sql.format(table, timestamp_column, clear_time_f, clear_time_t, openids)
                log.info(sql)
                model.execute(sql)
            except Exception as e:
                log.error("DDL delete error: %s", e)

    clear_dirs = [("consume", ddl.Consume), ("pay_orders", ddl.PayMent)]
    clean_dir = os.path.join(app.work_dir, "clean", ds)
    # 清理 csv
    with cd(clean_dir):
        for clear_dir, model in clear_dirs:
            csv_dir = os.path.join(clean_dir, clear_dir)
            for csv in os.listdir(csv_dir):
                # 指定单个 gameid
                if gameid:
                    # csv 不是此 gameid 的 csv 文件，不作处理
                    if not csv.startswith(gameid):
                        continue
                    filter_openids = game_openids.get(gameid)
                # 所有的 csv 文件都需要进行处理
                else:
                    filter_openids = game_openids.values()
                # gameid 的测试账号为空，不作处理
                if not filter_openids:
                    continue
                path = os.path.join(csv_dir, csv)
                df = util.read_csv(path, names=model.FIELDS, dtype=model.Dtype)
                df2 = df[~df["openid"].isin(filter_openids)]
                log.info("process csv path: %s, pre: %s, now: %s", path, len(df), len(df2))
                df2.to_csv(path, header=False, index=False, mode='w', encoding='utf8')


@manage.command
def uniq_history(app, gameid=None):
    """去除history重复的数据"""
    if gameid is None:
        games = util.get_gameid_from_history(app.history)
    else:
        games = [gameid]

    from bi.unit.login import LoginUnit
    from bi.unit.role import RoleUnit
    from bi.unit.payment import PaymentUnit

    historys = (
        (LoginUnit.HISTORY_LOGIN_CSV, LoginUnit.HISTORY_LOGIN_FIELDS),
        (LoginUnit.HISTORY_MAC_CSV, LoginUnit.HISTORY_MAC_FIELDS),
        (RoleUnit.HISTORY_CSV, RoleUnit.HISTORY_FIELDS),
        (PaymentUnit.HISTORY_CSV, PaymentUnit.HISTORY_FIELDS)
    )
    log.info("drop_history, gameids: %s", games)
    for game in games:
        # 处理csv
        for csv, fields in historys:
            path = os.path.join(app.history, game, csv)
            df = util.read_csv(path, names=fields)
            index = fields[:2]
            df2 = df.drop_duplicates(subset=index)
            log.info("process csv path: %s, pre: %s, now: %s", path, len(df), len(df2))
            df2.to_csv(path, header=False, index=False, mode='w', encoding='utf8')


@manage.command
def fix_date(app, start, model, fields, expression):
    """修改csv 当中的错误数据 model修改那个表 fields 修改哪个字段 expression 修改字段对应的表达式
    python manage.py fix_date 2016-08-24 PayMent amount "float(amount)*100"
    """
    model = getattr(ddl, model)
    table = model.TABLE_NAME
    with cd(app.work_dir):
        day = util.todate(start)
        directory = os.path.join(app.clean, str(day), table)
        files = os.listdir(directory)
        field = model.FIELDS
        fs = sorted(files)
        for f in fs:
            if "_" not in f or "_merge" in f or f.startswith('.') or "_bak" in f:
                continue
            file = os.path.join(directory, f)
            file_new = os.path.join(directory, "{0}_bak".format(f))
            f_new = open(file_new, "w")
            with open(file) as fi:
                for line in fi:
                    if line == "":
                        continue
                    row = line.strip().split(",")
                    kv = dict(zip(field, row))
                    index = field.index(fields)
                    row[index] = eval(expression, kv)
                    row[index] = str(row[index])
                    row_new = ','.join(row)
                    f_new.write(row_new + '\n')
            run("rm -rf {0}/{1}".format(directory, f))
            run("mv {0}/{1}_bak {0}/{1}".format(directory, f))


class Online(object):
    INTERVAL = 5

    def date_time(self, timestamp, delta=INTERVAL):
        """时间戳返回INTERVAL级别的date和time"""
        t = datetime.datetime.fromtimestamp(int(timestamp))
        tt = t.time()
        t2 = tt.replace(minute=tt.minute / delta * delta, second=0)
        return str(t.date()), str(t2)

    def cal_online(self, df):
        """计算在线时长"""
        group = defaultdict(dict)
        all_group = defaultdict(dict)

        for i, gameid, clientid, online_time, users in df.itertuples():
            try:
                users = int(users)
            except ValueError:
                users = 0
            dt = self.date_time(online_time)
            group[gameid].setdefault(clientid, {})
            group[gameid][clientid].update({dt: users})
            all_group[gameid].setdefault(dt, 0)
            all_group[gameid][dt] += users

        # storage
        group_list = []
        for gameid, values in group.iteritems():
            clientids = set()
            for clientid, v in values.iteritems():
                clientids.add(clientid)
                for (ds, ti), user in v.iteritems():
                    tmp = {"gameid": gameid, "clientid": clientid, "ds": ds, "ti": ti, "user": user}
                    group_list.append(tmp)

        for gameid, values in all_group.iteritems():
            print(gameid)
            for (ds, ti), user in values.iteritems():
                tmp = {"gameid": gameid, "clientid": 0, "ds": ds, "ti": ti, "user": user}
                group_list.append(tmp)

        # print(group_list)

        ddl.RealtimeOnline.insert(group_list)


@manage.command
def online(app, file):
    import pandas as pd
    df = pd.read_csv(file, names=ddl.Online.FIELDS)
    online = Online()
    online.cal_online(df)


@manage.command
def fix(app, start, end, **kwargs):
    """
    假如今日13号
    python manage.py fix 2015-12-11 2015-12-12(重跑这两天的csv)
    只修复11-12号的日志
    1、清空11-12的clean
    2、删除11-12号的mysql日志
    3、修复data数据，
        11号的date里面包含了10号的数据，删除11号data中10号的数据
        12号在13号的部分数据放到12号里面
        13/0000.log -> 12/最大的日志.log
    4、ex.py cron.py
    """
    models = [ddl.AllRole, ddl.AllUser, ddl.AllAdvice, ddl.AllPayUser, ddl.PayMent, ddl.Login, ddl.Consume,
              ddl.RoleNew, ddl.RoleLogin, ddl.Levelup, ddl.Online, ddl.Mission]

    ds_models = [ddl.RealtimeIncomeNewer, ddl.RealtimeOnline, ddl.RealtimeRegister]

    models.extend(ds_models)
    games = util.get_gameid_from_history(app.history)
    ensure(kwargs, "clear games:{0} start:{1}, end:{2} Please yes/no:".format(games, start, end))
    # 2删除数据库
    timestamp_day = util.timestamp(util.todate(start))
    tomorrow = util.date_delta(util.todate(end), 1)
    for game in games:
        for model in models:
            table = model.table_name(game)
            try:
                if model not in ds_models:
                    timestamp_columns = model.TIMESTAMP or filter(lambda x: x.endswith("_time"), model.FIELDS)[0]
                    sql = "delete from {0} where {1} >= {2} and {1} <{3}"
                    sql = sql.format(table, timestamp_columns, timestamp_day, tomorrow[1])
                else:
                    sql = "delete from {0} where gameid = {1} and ds >= '{2}' and ds <='{3}'"
                    sql = sql.format(table, game, start, end)
                log.info(sql)
                model.execute(sql)
            except Exception as e:
                log.error("DDL delete error: %s", e)

    with cd(app.work_dir):

        # 处理start
        day = util.todate(start)
        directory = os.path.join(app.data, str(day))
        files = os.listdir(directory)
        fs = sorted(files)
        day_bak = os.path.join(app.data, "{0}_startbak".format(str(day)))
        if not os.path.exists(day_bak):
            os.makedirs(day_bak)
        else:
            run("rm -rf {0}/*".format(day_bak))
        for f in fs:
            run("cat {0}/{1} | grep {2} > {3}/{1}".format(directory, f, day, day_bak))
        run("mv {0} {1}/{2}_bu".format(directory, app.data, day))
        run("mv {0} {1}/{2}".format(day_bak, app.data, day))

        if start == end:
            log.info("process if start == end is return start: %s end : %s", start, end)
            return

        # 处理end
        endday = util.todate(end)
        endtomorrow = util.todate(end) + datetime.timedelta(days=1)
        enddirectory = os.path.join(app.data, str(endtomorrow))
        daydirectory = os.path.join(app.data, str(endday))
        files = os.listdir(enddirectory)
        fs = sorted(files)
        end_bak = os.path.join(app.data, "{0}_endbak".format(str(endtomorrow)))
        if not os.path.exists(end_bak):
            os.makedirs(end_bak)
        else:
            run("rm -rf {0}/*".format(end_bak))
        run("cat {0}/* | grep {1} > {2}/{3}".format(enddirectory, end, end_bak, fs[-1]))
        run("cat {0}/{2} >> {1}/{2}".format(end_bak, daydirectory, fs[-1]))

        for i in range((util.todate(end) - util.todate(start)).days + 1):
            day = util.todate(start) + datetime.timedelta(days=i)
            run("rm -rf clean/{0}".format(str(day)))
            # run("python {0}/ex.py {1}".format(app.shell,day))
            # run("python {0}/cron.py {1}".format(app.shell,day))


if __name__ == "__main__":
    manage.run()
