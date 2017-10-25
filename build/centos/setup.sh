set -e
cd $HOME

# Need to build on an old Linux version to support older GLIBC versions.
# Need a modern C++ version to compile Rocks.
# Fun!
sudo wget https://people.centos.org/tru/devtools-2/devtools-2.repo -O /etc/yum.repos.d/devtools-2.repo

echo === Installing necessary system libraries to build RocksDB
sudo yum update -y
sudo yum install -y git devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ devtoolset-2-libstdc++-devel

echo "source /opt/rh/devtoolset-2/enable" >> ~/.bash_profile
source /opt/rh/devtoolset-2/enable

echo === Building RocksDB
# download and compile rocksdb 5.7.3
if [ ! -f ~/rocksdb/librocksdb.a ]; then
  git clone https://github.com/facebook/rocksdb
  cd rocksdb
  git checkout v5.7.3
  make libsnappy.a DEBUG_LEVEL=0 PORTABLE=1
  make static_lib PORTABLE=1
  # default binary is 340MB!
  # stripped is 18MB
  strip -g librocksdb.a
fi

echo === Installing Golang
# download and install go 1.9
if [ ! -d /usr/local/go ]; then
  cd /usr/local
  curl https://storage.googleapis.com/golang/go1.9.1.linux-amd64.tar.gz | sudo tar xfz -
  echo "export PATH=/usr/local/go/bin:$HOME/go/bin:\$PATH" >> ~/.bash_profile
  export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH
fi

mkdir -p ~/go/src/github.com/contribsys
cd ~/go/src/github.com/contribsys && ln -s /faktory faktory && cd faktory

# download project dependencies
export ROCKSDB_HOME="$HOME/rocksdb"
export CGO_CFLAGS="-I$ROCKSDB_HOME/include"
export CGO_LDFLAGS="-L$ROCKSDB_HOME"
echo === Installing dependencies
make prepare

# build faktory
echo === Running Faktory test suite
make
make build
