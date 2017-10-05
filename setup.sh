set -e
cd $HOME

sudo apt-get update
sudo apt install -y --no-install-recommends make g++ ruby zlib1g-dev libbz2-dev libsnappy-dev

# download and compile rocksdb 5.7.3
if [ ! -f ~/rocksdb/librocksdb.a ]; then
  git clone https://github.com/facebook/rocksdb
  cd rocksdb
  git checkout v5.7.3
  time make static_lib
  # default binary is 340MB!
  # stripped is 18MB
  strip -g librocksdb.a
fi

# download and install go 1.9
if [ ! -d /usr/local/go ]; then
  cd /usr/local
  curl https://storage.googleapis.com/golang/go1.9.1.linux-amd64.tar.gz | sudo tar xfz -
  echo "PATH=/usr/local/go/bin:$HOME/go/bin:\$PATH" >> ~/.profile
  export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH
fi

cd /vagrant

# download project dependencies
export ROCKSDB_HOME="/home/ubuntu/rocksdb"
export CGO_CFLAGS="-I${ROCKSDB_HOME}/include"
export CGO_LDFLAGS="-L${ROCKSDB_HOME}"
make prepare

# build faktory
cd ~/go/src/github.com/mperham && ln -s /vagrant faktory && cd faktory
make
