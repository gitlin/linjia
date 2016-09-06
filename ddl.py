# coding=utf8

from functools import partial

import torndb

import config
from log import log

"""
数据库定义
"""


class DDL(object):
    TABLES = {}
    OLD_TABLES = {}
    NEW_TABLES = {}
    CONNECTIONS = {}

    @classmethod
    def connection(cls, db):
        """默认为统计的库
        db config.DB_ANALYSE, config.DB_STORE
        """
        if db not in cls.CONNECTIONS:
            get = partial(config.get, db)
            log.warning("mysql connection init, db: %s", db)
            conn = torndb.Connection(get("host"), get("db"), user=get("user"), password=get("password"))
            cls.CONNECTIONS[db] = conn
            return conn
        return cls.CONNECTIONS[db]

    @classmethod
    def execute(cls, sql, db):
        try:
            cls.connection(db).execute(sql)
        except Exception as e:
            log.error("DDL execute error: %s, db: %s, sql:\n%s", str(e), db, sql)

    @classmethod
    def create(cls, table, gameid=None, date=None):
        model = cls.TABLES.get(table)
        if model is None:
            log.error("DDL create error: model is None")
            return None
        if gameid is not None and model.NEED_GAMEID:
            if date is not None:
                table = "{0}_{1}_{2}".format(gameid, table, date)
            else:
                table = "{0}_{1}".format(gameid, table)

        try:
            desc_sql = '''desc `{0}`'''.format(table)
            cls.connection(model.DB).execute(desc_sql)
        except Exception as e:
            log.warning(e)
            fmt = '''create table if not exists `{0}`({1}'''.format(table, model.CREATE_SQL)
            cls.connection(model.DB).execute(fmt)
            log.info("table %s is created." % table)
        else:
            log.info("table %s is already exist." % table)
        return True

    @classmethod
    def insert(cls, table, groupdict, gameid=None):
        model = cls.TABLES[table]
        model.insert(groupdict, gameid)

    @classmethod
    def mysql_load_data(cls, table, csv, gameid=None, db=None, ignore="", field=None, date=None):
        if isinstance(table, str):
            model = cls.TABLES.get(table)
        else:
            model = table

        if model is None:
            log.error("DDL create error: model is None")
            return ""

        get = partial(config.get, model.DB)
        if db is None:
            db = get("db")
        password = get("password")
        if password != "":
            passwd = "-p{0}".format(password)
        else:
            passwd = ""
        table = model.TABLE_NAME
        cls.create(table, gameid, date)

        if gameid is not None and model.NEED_GAMEID:
            if date is not None:
                table = "{0}_{1}_{2}".format(gameid, table, date)
            else:
                table = "{0}_{1}".format(gameid, table)

        fields = field or model.FIELDS
        if ignore:
            ignore = "ignore {0} lines".format(ignore)

        cmd = '''mysql -u{0} {1} -h {2} {3} --local-infile=1 -e "load data local infile '{4}' ignore into table \`{5}\` \
            character set utf8 fields terminated by ',' optionally enclosed by '\\"' escaped by '\\"' \
            lines terminated by '\\n' {6} ({7});"\
            '''.format(get("user"), passwd, get("host"), db, csv, table, ignore, ','.join(fields))

        return cmd


class TableMeta(type):
    def __init__(cls, classname, bases, dict_):
        assert hasattr(cls, "TABLE_NAME")
        if classname in ("Model", "Merge"):
            return type.__init__(cls, classname, bases, dict_)

        table = getattr(cls, "TABLE_NAME")
        instance = cls()
        if table in DDL.TABLES:
            # may two class has the same TABLE_NAME, just warning
            print("[warning] duplicate table template:{0}, class:{1}".format(table, classname))
        else:
            DDL.TABLES[table] = instance

        if hasattr(cls, "OLDBI_TABLE"):
            DDL.OLD_TABLES[getattr(cls, "OLDBI_TABLE")] = instance
        if hasattr(cls, "NEWBI_TABLE"):
            DDL.NEW_TABLES[getattr(cls, "NEWBI_TABLE")] = instance
        if hasattr(cls, "CREATE_SQL"):
            fields = check_fields(getattr(cls, "CREATE_SQL"))
            setattr(cls, "FIELDS", fields)

        return type.__init__(cls, classname, bases, dict_)


def check_fields(sql):
    """ get table fields from create sql """
    fields = []
    for line in sql.splitlines():
        row = line.strip()
        if row[0] != '`' or "AUTO_INCREMENT" in row:
            continue
        fields.append(row.split('`', 2)[1])

    return fields


def format_sql(group, fields):
    values = ["'{0}'".format(group.get(f, 0)) for f in fields]
    return "({0})".format(','.join(values))


class Model(object):
    __metaclass__ = TableMeta
    TABLE_NAME = ""        # 数据库表名
    OLDBI_TABLE = ""       # 老日记对应
    NEWBI_TABLE = ""       # 新日志对应
    CREATE_SQL = ""
    DEFAULTS = {}          # 字段默认值
    TIMESTAMP = ""         # 时间字段(方便老日志date+time合并为timestamp)
    NEED_GAMEID = True     # 是否是一个gameid一张表(只有统计表不是)
    DB = config.DB_STORE   # model默认是入库的库
    Dtype = {"roleid": str, "openid": str, "clientid": int}
    CLIENT_SPLIT = False   # 是否按区服分割数据
    STORE = True           # 是否入库

    @classmethod
    def table_name(cls, gameid=None):
        table = cls.TABLE_NAME
        if gameid is not None and cls.NEED_GAMEID:
            table = "{0}_{1}".format(gameid, table)
        return table

    @classmethod
    def insert(cls, groupdict, gameid=None):
        if not groupdict:
            return
        DDL.create(cls.TABLE_NAME, gameid)
        if isinstance(groupdict, (list, tuple)):
            values = ','.join([format_sql(group, cls.FIELDS) for group in groupdict])
        else:
            values = format_sql(groupdict, cls.FIELDS)

        table = cls.table_name(gameid)
        sql = """insert into {0}({1}) values{2}""".format(table, ','.join(cls.FIELDS), values)
        try:
            DDL.connection(cls.DB).execute(sql)
        except Exception as e:
            log.error("table:%s insert error, %s, sql:\n%s", table, str(e), sql)

    @classmethod
    def query(cls, sql):
        return DDL.connection(cls.DB).query(sql)

    @classmethod
    def execute(cls, sql):
        return DDL.execute(sql, cls.DB)

    @classmethod
    def drop(cls, gameid=None):
        """删除表"""
        sql = """drop table if exists {0}"""
        sql = sql.format(cls.table_name(gameid))
        print(sql)
        cls.execute(sql)

    @classmethod
    def delete(cls, gameids, **where):
        if not isinstance(gameids, (list, tuple, set)):
            gameids = [gameids]
        # where_clause = " and ".join("{0}='{1}'".format(k, v) for k, v in where.iteritems())
        where_clause = " and ".join("{0}='{1}'".format(k, v) if not isinstance(v, (list, tuple, set)) else "{0} in({1})".format(k, ",".join(map(str, v))) for k, v in where.iteritems())
        sql = """delete from {0} where {1} and gameid in({2})"""
        sql = sql.format(cls.table_name(), where_clause, ",".join(gameids))
        # print(sql)
        cls.execute(sql)

    @classmethod
    def update(cls, groupdict, where, gameid=None):
        if isinstance(groupdict, dict):
            groupdict = [groupdict]
        table = cls.table_name(gameid)
        for group in groupdict:
            where_clause = " and ".join(["{0}='{1}'".format(f, group[f]) for f in where])
            sql = """select 1 from {0} where {1}""".format(table, where_clause)
            raw_count = DDL.connection(cls.DB).execute_rowcount(sql)
            if raw_count < 1:
                cls.insert(group, gameid=gameid)
            else:
                values = ",".join(["{0}='{1}'".format(f, group[f]) for f in group.keys() if f not in where])
                sql = """update {0} set {1} where {2}""".format(table, values, where_clause)
                try:
                    DDL.connection(cls.DB).execute_rowcount(sql)
                except Exception as e:
                    log.error("table:%s update error,%s, sql:\n%s", table, str(e), sql)


class Merge(Model):
    """ 统计表 """
    NEED_GAMEID = False
    DB = config.DB_ANALYSE    # model默认使用统计的库


_all_user_sql = '''\
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `openid` varchar(100) NOT NULL COMMENT '用户平台账号',
  `snid` int(11) NOT NULL COMMENT '平台ID',
  `login_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `openid_snid` (`openid`,`snid`),
  KEY `login_time` (`login_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='所有用户表' '''
class AllUser(Model):
    TABLE_NAME = "all_user"
    CREATE_SQL = _all_user_sql



_props_get_day_sql = '''\
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `ds` date DEFAULT NULL,
  `dimension` smallint(6) DEFAULT NULL COMMENT 'dau1,新用户2,首付费3,流水4,历史付费7',
  `gameid` varchar(32) NOT NULL COMMENT '游戏ID',
  `clientid` int(11) NOT NULL COMMENT '区服ID',
  `propsid` varchar(200) NOT NULL COMMENT '道具ID',
  `type` varchar(200) NOT NULL COMMENT '道具特征(bind，unbind)',
  `get_wayid` int(11) NOT NULL COMMENT '获得方式',
  `get_wayclassid` int(11) NOT NULL COMMENT '获得方式所属分类(任务，拍卖行)',
  `total_cnt` int(11) NOT NULL COMMENT '人次',
  `unique_cnt` int(11) NOT NULL COMMENT '人数',
  `props_sum` int(11) NOT NULL COMMENT '获取道具总数量',
  PRIMARY KEY (`id`),
  KEY `dx1` (`ds`,`gameid`,`dimension`, `clientid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='道具产出表' '''
class PropsGetDay(Merge):
    TABLE_NAME = "props_get_day"
    OLDBI_TABLE = TABLE_NAME
    NEWBI_TABLE = TABLE_NAME
    CREATE_SQL = _props_get_day_sql




_gold_consume_day_sql = '''\
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `ds` date DEFAULT NULL,
  `dimension` smallint(6) DEFAULT NULL COMMENT 'dau1,新用户2,首付费3,流水4,历史付费7',
  `gameid` varchar(32) NOT NULL COMMENT '游戏ID',
  `clientid` int(11) NOT NULL COMMENT '区服ID',
  `goodsid` int(11) NOT NULL COMMENT '物品',
  `consume_wayid` int(11) NOT NULL COMMENT '获得方式',
  `consume_wayclassid` int(11) NOT NULL COMMENT '获得方式所属分类(任务，拍卖行)',
  `total_cnt` bigint(20) NOT NULL COMMENT '人次',
  `unique_cnt` int(11) NOT NULL COMMENT '人数',
  `gold_sum` bigint(20) NOT NULL COMMENT '获取金币总数量',
  `poundage` bigint(20) NOT NULL COMMENT '手续费总数量',
  `goodsnum` bigint(20) NOT NULL COMMENT '物品总数量',
  PRIMARY KEY (`id`),
  KEY `dx1` (`ds`,`gameid`,`dimension`, `clientid`, `goodsid`, `consume_wayclassid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='道具消耗日统计表' '''
class GoldConsumeDay(Merge):
    TABLE_NAME = "gold_consume_day"
    OLDBI_TABLE = TABLE_NAME
    NEWBI_TABLE = TABLE_NAME
    CREATE_SQL = _gold_consume_day_sql




def test():
    print(DDL.create("user", "2100007", True))


def test_check_field():
    print(check_fields(_login_month_sql))


def test_insert():
    ActiveMonth.insert({"ds": "2015-08-25", "gameid": "2100007", "snid": 11, "user_pay_cnt": 100})

if __name__ == "__main__":
    test_insert()
