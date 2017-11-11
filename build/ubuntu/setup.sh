set -e
cd $HOME

echo === Installing necessary system libraries to build RocksDB
sudo apt-get update -y
sudo apt install -y --no-install-recommends make g++

echo === Building RocksDB
# download and compile rocksdb 5.7.3
if [ ! -f ~/rocksdb/librocksdb.a ]; then
  git clone https://github.com/facebook/rocksdb
  cd rocksdb
  git checkout v5.7.3
  time PORTABLE=1 make static_lib
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
export CGO_CFLAGS="-I${ROCKSDB_HOME}/include"
export CGO_LDFLAGS="-L${ROCKSDB_HOME}"
echo === Installing dependencies
make prepare

echo === Running Faktory test suite
make
make build

# If you wish to build a .deb package, run these commands
# sudo apt install ruby-dev --no-install-recommends -y
# sudo gem install -N fpm
# make deb
