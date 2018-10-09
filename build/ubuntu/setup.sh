set -e
cd $HOME

echo === Installing necessary system libraries to build
sudo apt-get update -y
sudo apt install -y --no-install-recommends make redis-server

echo === Installing Golang
# download and install go 1.10
if [ ! -d /usr/local/go ]; then
  cd /usr/local
  curl https://storage.googleapis.com/golang/go1.10.3.linux-amd64.tar.gz | sudo tar xfz -
  echo "export PATH=/usr/local/go/bin:$HOME/go/bin:\$PATH" >> ~/.bash_profile
  export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH
fi

mkdir -p ~/go/src/github.com/contribsys
cd ~/go/src/github.com/contribsys && ln -s /faktory faktory && cd faktory

# download project dependencies
echo === Installing dependencies
make prepare

echo === Running Faktory test suite
make test
make build

# If you wish to build a .deb package, run these commands
# sudo apt install ruby-dev --no-install-recommends -y
# sudo gem install -N fpm
# make deb
# dpkg-deb -I <filename> to see details of built .DEB
