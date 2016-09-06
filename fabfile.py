#!/usr/bin/env python
# encoding: utf-8

from prettytable import PrettyTable

from fabric.api import env, run, local, task
from fabric.operations import put, get, open_shell, prompt
from fabric.context_managers import cd, lcd, warn_only
from fabric.contrib.console import confirm


# 本地代码库目录
repository = '/home/skycrab/lingit/python'

# 首都机房服务器密码
passwd_sd = "666666666655555"


servers = {
    "34": {
        "host": "33.33.444.7",
        "port": 7655,
        "intro": u"BI web 服务器",
        "shell_dir": "-"
    },
  

}


class DeployShell():
    def __init__(self):
        self.host = None
        self.port = 22
        self.user = "root"
        self.passwd = "555tttttttt"
        self.shell_dir = "/data4/bi/shell"
        self.intro = ""
        self.set_up()

    def set_up(self):
        self.print_servers()
        self.check_params()

    def check_params(self):
        prompt("请输入需要连接/更新的远程服务器（简称）：", validate=self.update_attr)

    def print_servers(self):
        header = [u"远程服务器", u"密码", u"简称", u"备注", u"shell 目录"]
        table = PrettyTable(header)
        s = sorted(servers.iteritems(), key=lambda x: x[1]["host"])
        # for k, v in servers.iteritems():
        for k, v in s:
            table.add_row([
                v.get("user", "root") + "@" + v["host"] + ":" + str(v.get("port", 22)),
                v.get("passwd", self.passwd),
                ",".join([k] + v.get("aliases", [])),
                v["intro"],
                v.get("shell_dir", self.shell_dir)
            ])
        print table

    def update_attr(self, param):
        params = {}
        for k, v in servers.iteritems():
            params[k] = k
            for alias in v.get("aliases", []):
                params[alias] = k
        if param in params:
            name = params[param]
            self.host = servers[name]["host"]
            self.host = servers[name].get("host", self.port)
            self.port = servers[name].get("port", self.port)
            self.user = servers[name].get("user", self.user)
            self.passwd = servers[name].get("passwd", self.passwd)
            self.shell_dir = servers[name].get("shell_dir", self.shell_dir)
            self.intro = servers[name].get("intro", self.intro)
        else:
            # self.print_servers()
            raise Exception("输入的服务器简称无效！")
        env.host_string = "{self.user}@{self.host}:{self.port}".format(self=self)
        env.password = self.passwd
        return param

    def update_shell(self, *args, **kwargs):
        """
        fab update:*                            # 拷贝整个目录，同时会将 script 中的文件全部拷贝出来
        fab update:bi,gitbranch=develop         # 拷贝 bi 目录，并指定 develop 分支
        fab update:bi/unit                      # 拷贝 bi 目录下的 unit 目录，要确保 bi 目录在远程服务器中存在
        fab update:manage.py,main.py            # 拷贝 manage.py 和 main.py 这两个文件
        fab update:bi/unit/month_consume.py     # 拷贝 bi/unit 目录下的 month_consume.py 文件，同样要确保 bi/unit 目录存在
        """
        print self.update_shell.__doc__
        question = "确认更新服务器？({self.host}) ".format(self=self)
        if not confirm(question, default=False):
            return
        with warn_only():
            with cd('%s/analysis' % self.shell_dir):
                run('sudo chattr -i -R ../analysis')
                with lcd('%s/analysis' % repository):
                    # 从本地拷贝源码文件到服务器
                    # 指定分支后会先 pull 然后再拷贝
                    # 注意：此步会将本地代码仓库的修改清理掉，注意提交保存
                    gitbranch = kwargs.get('gitbranch')
                    if gitbranch:
                        local('git checkout -q master')
                        local('git pull')
                        local('git reset --hard origin/master')
                        local('git branch -D %s 2>/dev/null' % gitbranch)
                        local('git checkout -q -b %s origin/%s' % (gitbranch, gitbranch))
                    # 清理本地 pyc 文件，防止拷贝到远程服务器上
                    local('find ./ -type f -name "*.pyc" | xargs sudo rm -f')
                    for file in args:
                        # 处理文件路径
                        info = file.rsplit('/', 1)
                        fdir = '.'
                        if len(info) == 2:
                            fdir = info[0]
                        # 拷贝文件到服务器
                        put(file, fdir, use_sudo=True)
                        # 如果是 script 目录下文件，还需要将其拷贝出来
                        if file == 'script' or fdir == 'script' or file == '*':
                            run('cp script/* ./')
                # 清理一下远程服务器上的 pyc 文件
                run('find ./ -type f -name "*.pyc" | xargs sudo rm -f')
                # 通过参数 chattr_i 来判定是否设置目录 immutable 属性
                chattr_i = kwargs.get('chattr_i', 'true')
                if chattr_i == 'true':
                    run('sudo chattr +i -R ../analysis')

    def ssh_remote(self):
        open_shell()
        # 用这个在远程服务器按上下来选择历史执行的命令的时候会有点问题，总会慢一拍~~

    def spawn_remote(self):
        import os
        # ssh 连接远程服务器
        self.passwd = self.passwd.replace("$", "\$")
        cmd = """
        expect -c '
            spawn ssh {self.user}@{self.host} -p {self.port}
            expect {{
                "*yes/no" {{ send "yes\r"; exp_continue }}
                "*password:" {{ send "{self.passwd}\r" }}
            }}
            interact
        '
        """.format(self=self)
        os.system(cmd)

    def pxssh_remote(self):
        from pexpect import pxssh
        import struct, fcntl, termios, signal, sys

        try:
            p = pxssh.pxssh()

            def reset_winsize():
                s = struct.pack("HHHH", 0, 0, 0, 0)
                a = struct.unpack('hhhh', fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, s))
                p.setwinsize(a[0], a[1])

            def sigwinch_passthrough(sig, data):
                reset_winsize()

            p.login(self.host, self.user, self.passwd, port=self.port, auto_prompt_reset=False)
            reset_winsize()
            signal.signal(signal.SIGWINCH, sigwinch_passthrough)
            p.interact()
            p.logout()
        except pxssh.ExceptionPxssh as e:
            print("pxssh failed on login.")
            print(e)

    def start_crond(self):
        run('sudo /etc/init.d/crond start')

    def stop_crond(self):
        run('sudo /etc/init.d/crond stop')

    def upload(self, *args, **kwargs):
        fdir = kwargs.get('fdir', '/root')
        for file in args:
            # 拷贝文件到服务器
            put(file, fdir, use_sudo=True)

    def download(self, *args, **kwargs):
        lpath = kwargs.get('lpath', './')
        for file in args:
            # get(remote_path=f, local_path=lpath, use_sudo=True)
            get(file, lpath, use_sudo=True)


@task
def update(*args, **kwargs):
    """更新脚本"""
    deploy = DeployShell()
    deploy.stop_crond()
    deploy.update_shell(*args, **kwargs)
    deploy.start_crond()


@task
def ssh():
    """登录"""
    deploy = DeployShell()
    # deploy.ssh_remote()
    deploy.spawn_remote()
    # deploy.pxssh_remote()


@task
def upload(*args, **kwargs):
    """上传文件"""
    deploy = DeployShell()
    deploy.upload(*args, **kwargs)


@task
def download(*args, **kwargs):
    """下载文件"""
    deploy = DeployShell()
    deploy.download(*args, **kwargs)
