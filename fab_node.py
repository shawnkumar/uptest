from fabric.api import *
from StringIO import StringIO
import yaml

def start():
    run('JAVA_HOME=~/fab/java nohup ~/fab/cassandra/bin/cassandra')

def stop():
    with settings(warn_only=True):
        run('pkill -9 -f "java.*org.apache.*.CassandraDaemon"')

def update(revision):
    # make sure cassandra is dead
    
    with settings(warn_only=True):
        run('pgrep -f cassa | xargs kill')

    topIO = StringIO()
    get('~/fab/cassandra/conf/cassandra-topology.properties', topIO)
    topIO.seek(0)
    topology = topIO.read()

    rackdcIO = StringIO()
    get('~/fab/cassandra/conf/cassandra-rackdc.properties', rackdcIO)
    rackdcIO.seek(0)
    rackdc = rackdcIO.read()

    yamlIO = StringIO()
    get('~/fab/cassandra/conf/cassandra.yaml', yamlIO)
    yamlIO.seek(0)
    config = yaml.load(yamlIO.read())

    git_checkout_status = run('test -d ~/fab/cassandra.git', quiet=True)
    if git_checkout_status.return_code > 0:
        run('git init --bare ~/fab/cassandra.git')
        for name,url in git_repos:
            run('git --git-dir=$HOME/fab/cassandra.git remote add {name} {url}'.format(name=name, url=url), quiet=True)
        for name,url in reversed(git_repos):
            run('git --git-dir=$HOME/fab/cassandra.git fetch {name}'.format(name=name))

    run('rm -rf ~/fab/cassandra')

    # Find the SHA for the revision requested:
    git_id = run('git --git-dir=$HOME/fab/cassandra.git rev-parse {revision}'.format(revision=revision)).strip()

    # Build Cassandra Checkout revision/tag:
    run('mkdir ~/fab/cassandra')
    run('git --git-dir=$HOME/fab/cassandra.git archive %s | tar x -C ~/fab/cassandra' % revision)
    run('echo -e \'%s\\n%s\\n\' > ~/fab/cassandra/0.GIT_REVISION.txt' % (revision, git_id))
    run('JAVA_HOME=~/fab/java ~/fab/ant/bin/ant -f ~/fab/cassandra/build.xml clean')
    run('JAVA_HOME=~/fab/java ~/fab/ant/bin/ant -f ~/fab/cassandra/build.xml')

    # Save config:
    conf_file = StringIO()
    conf_file.write(yaml.safe_dump(config, encoding='utf-8', allow_unicode=True))
    conf_file.seek(0)
    put(conf_file, '~/fab/cassandra/conf/cassandra.yaml')

    topology_file = StringIO()
    topology_file.write(topology)
    topology_file.seek(0)
    put(topology_file, '~/fab/cassandra/conf/cassandra-topology.properties')

    rackdc_file = StringIO()
    rackdc_file.write(rackdc)
    rackdc_file.seek(0)
    put(rackdc_file, '~/fab/cassandra/conf/cassandra-rackdc.properties')

def get_log(address):
    path = get(remote_path='~/fab/cassandra/logs/system.log', local_path='~/fab/'+ address + '.log')
    return path[0]