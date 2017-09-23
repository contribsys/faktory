set -e
cd $HOME

sudo apt install -y --no-install-recommends make g++ ruby zlib1g-dev libbz2-dev libsnappy-dev

# download and compile rocksdb 5.7.3
if [ ! -d rocksdb ]; then
  git clone https://github.com/facebook/rocksdb
  cd rocksdb
  git checkout v5.7.3
  make static_lib
  # default binary is 340MB!
  # stripped is 18MB
  strip -g librocksdb.a
fi

# download and install go 1.9
if [ ! -d go ]; then
  curl https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz | tar xfz -
  echo "PATH=$HOME/go/bin:\$PATH" >> ~/.profile
  echo "GOROOT=$HOME/go" >> ~/.profile
fi

cd /vagrant
# download project dependencies
make prepare
cd ~/src/github.com/mperham && ln -s /vagrant faktory && cd faktory
# build faktory
#make
