set -e
cd $HOME

sudo yum update -y
sudo yum install -y git epel-release
sudo yum install -y redis

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
