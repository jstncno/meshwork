# ~/.profile: executed by the command interpreter for login shells.
# This file is not read by bash(1), if ~/.bash_profile or ~/.bash_login
# exists.
# see /usr/share/doc/bash/examples/startup-files for examples.
# the files are located in the bash-doc package.

# the default umask is set in /etc/profile; for setting the umask
# for ssh logins, install and configure the libpam-umask package.
#umask 022

# if running bash
if [ -n "$BASH_VERSION" ]; then
    # include .bashrc if it exists
    if [ -f "$HOME/.bashrc" ]; then
	. "$HOME/.bashrc"
    fi
fi

# set PATH so it includes user's private bin if it exists
if [ -d "$HOME/bin" ] ; then
    PATH="$HOME/bin:$PATH"
fi

export MASTER_NAME=<MASTER_PRIVATE_IP>
export MASTER_PUBLIC_IP=<MASTER_PUBLIC_IP>

# Requires Java 7
# sudo apt-get install openjdk-7-jdk
export JAVA_HOME=/usr
export PATH=$PATH:$JAVA_HOME/bin

export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

export AWS_ACCESS_KEY_ID='<AWS_ACCESS_KEY_ID>'
export AWS_SECRET_ACCESS_KEY='<AWS_SECRET_ACCESS_KEY>'

export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin

export HBASE_HOME=/usr/local/hbase
export PATH=$PATH:$HBASE_HOME/bin

alias spark-shell='$SPARK_HOME/bin/spark-shell --master spark://$($MASTER_NAME):7077' 

PS1="master:~ $"
changePrompt() {
  if [ "$PWD" == "$HOME" ]; then
    PS1="master:~ $ "
  else
    PS1="master:${PWD##*/} $ "
  fi
}

export PROMPT_COMMAND=changePrompt

# Startup scripts
# Hadoop
#$HADOOP_HOME/sbin/start-dfs.sh
#$HADOOP_HOME/sbin/start-yarn.sh
#$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

# Check to see if all the processes are running on the namenode by running
jps

# Spark
#$SPARK_HOME/sbin/start-all.sh

# Zookeeper
#sudo /usr/local/zookeeper/bin/zkServer.sh start

# HBase
#$HBASE_HOME/bin/start-hbase.sh
# HBase REST API (Stargate)
#$HBASE_HOME/bin/hbase-daemon.sh start rest -p 10001
