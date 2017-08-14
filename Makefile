NAME=faktory
VERSION=0.0.1

# when fixing packaging bugs but not changing the binary, we increment ITERATION
ITERATION=1
BASENAME=$(NAME)_$(VERSION)-$(ITERATION)

# contains various secret or machine-specific variables.
# DEB_PRODUCTION: hostname of a debian-based upstart machine (e.g. Ubuntu {12,14}.04 LTS)
# RPM_PRODUCTION: hostname of a redhat-based systemd machine (e.g. CentOS 7)
-include .local.sh

# TODO I'd love some help making this a proper Makefile
# with real file dependencies.

all: test

prepare:
	#wget https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz
	#sudo tar -C /usr/local -xzf go1.5.1.linux-amd64.tar.gz
	go get github.com/stretchr/testify/...
	go get github.com/jteeuwen/go-bindata/...
	go get -u github.com/kardianos/govendor
	#linters
	go get github.com/alecthomas/gometalinter
	#you must have .local.sh with ROCKSDB_HOME set
	#export ROCKSDB_HOME=/usr/local/Cellar/rocksdb/5.5.1
	# brew install rocksdb zstd
	#export CGO_CFLAGS="-I${ROCKSDB_HOME}/include"
	#export CGO_LDFLAGS="-L${ROCKSDB_HOME} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
	go get github.com/tecbot/gorocksdb
	gometalinter --install
	gem install -N fpm
	@echo Now you should be ready to run "make"
	# To cross-compile from OSX to Linux, you need to run this:
	#   cd \$GOROOT/src && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 ./make.bash --no-clean
	# or ensure your Homebrew'd Go can cross compile:
	#   brew install go --with-cc-common

test: clean
	go generate ./...
	go test -parallel 4 ./...

build: test
	@GOOS=linux GOARCH=amd64 go build -o faktory cmd/main.go

# goimports produces slightly different formatted code from go fmt
fmt:
	go fmt ./...

lint:
	gometalinter ./...

clean:
	rm webui/*.ego.go
	rm -rf tmp
	rm -f main faktory templates.go
	rm -rf packaging/output
	mkdir -p packaging/output/upstart
	mkdir -p packaging/output/systemd

run: clean
	go run cmd/main.go -l debug -s i.sock -d .

package: clean version_check build_deb build_rpm

version_check:
	@grep -q $(VERSION) faktory.go || (echo VERSIONS OUT OF SYNC && false)

purge_deb:
	ssh -t $(DEB_PRODUCTION) 'sudo apt-get purge -y $(NAME) && sudo rm -f /etc/inspeqtor' || true

purge_rpm:
	ssh -t $(RPM_PRODUCTION) 'sudo rpm -e $(NAME) && sudo rm -f /etc/inspeqtor' || true

deploy_deb: clean build_deb purge_deb
	scp packaging/output/upstart/*.deb $(DEB_PRODUCTION):~
	ssh $(DEB_PRODUCTION) 'sudo rm -f /etc/inspeqtor && sudo dpkg -i $(NAME)_$(VERSION)-$(ITERATION)_amd64.deb && sudo ./fix && sudo restart inspeqtor || true'

deploy_rpm: clean build_rpm purge_rpm
	scp packaging/output/systemd/*.rpm $(RPM_PRODUCTION):~
	ssh -t $(RPM_PRODUCTION) 'sudo rm -f /etc/inspeqtor && sudo yum install -q -y $(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm && sudo ./fix && sudo systemctl restart inspeqtor'

update_deb: clean build_deb
	scp packaging/output/upstart/*.deb $(DEB_PRODUCTION):~
	ssh $(DEB_PRODUCTION) 'sudo dpkg -i $(NAME)_$(VERSION)-$(ITERATION)_amd64.deb'

update_rpm: clean build_rpm
	scp packaging/output/systemd/*.rpm $(RPM_PRODUCTION):~
	ssh -t $(RPM_PRODUCTION) 'sudo yum install -q -y $(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm'

deploy: deploy_deb deploy_rpm
purge: purge_deb purge_rpm

cover:
	go test -cover -coverprofile cover.out
	go tool cover -html=cover.out

build_rpm: build_rpm_upstart build_rpm_systemd
build_deb: build_deb_upstart build_deb_systemd

build_rpm_upstart: build
	# gem install fpm
	# brew install rpm
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p packaging/output/upstart \
		--rpm-compression bzip2 --rpm-os linux \
	 	--after-install packaging/scripts/postinst.rpm.upstart \
	 	--before-remove packaging/scripts/prerm.rpm.upstart \
		--after-remove packaging/scripts/postrm.rpm.upstart \
		--url http://contribsys.com/inspeqtor \
		--description "Application infrastructure monitoring" \
		-m "Contributed Systems LLC <oss@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		inspeqtor=/usr/bin/inspeqtor \
		packaging/root/=/

build_rpm_systemd: build
	# gem install fpm
	# brew install rpm
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p packaging/output/systemd \
		--rpm-compression bzip2 --rpm-os linux \
	 	--after-install packaging/scripts/postinst.rpm.systemd \
	 	--before-remove packaging/scripts/prerm.rpm.systemd \
		--after-remove packaging/scripts/postrm.rpm.systemd \
		--url http://contribsys.com/inspeqtor \
		--description "Application infrastructure monitoring" \
		-m "Contributed Systems LLC <oss@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		inspeqtor=/usr/bin/inspeqtor \
		packaging/root/=/

# TODO build_deb_systemd
build_deb_upstart: build
	# gem install fpm
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p packaging/output/upstart \
		--deb-priority optional --category admin \
		--deb-compression bzip2 \
	 	--after-install packaging/scripts/postinst.deb.upstart \
	 	--before-remove packaging/scripts/prerm.deb.upstart \
		--after-remove packaging/scripts/postrm.deb.upstart \
		--url http://contribsys.com/inspeqtor \
		--description "Application infrastructure monitoring" \
		-m "Contributed Systems LLC <oss@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		inspeqtor=/usr/bin/inspeqtor \
		packaging/root/=/

build_deb_systemd: build
	# gem install fpm
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p packaging/output/systemd \
		--deb-priority optional --category admin \
		--deb-compression bzip2 \
	 	--after-install packaging/scripts/postinst.deb.systemd \
	 	--before-remove packaging/scripts/prerm.deb.systemd \
		--after-remove packaging/scripts/postrm.deb.systemd \
		--url http://contribsys.com/inspeqtor \
		--description "Application infrastructure monitoring" \
		-m "Contributed Systems LLC <oss@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		inspeqtor=/usr/bin/inspeqtor \
		packaging/root/=/

tag:
	git tag v$(VERSION)-$(ITERATION) && git push --tags || :

upload:	package tag
	# gem install -N package_cloud
	package_cloud push contribsys/inspeqtor/ubuntu/xenial packaging/output/systemd/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb
	package_cloud push contribsys/inspeqtor/ubuntu/precise packaging/output/upstart/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb
	package_cloud push contribsys/inspeqtor/ubuntu/trusty packaging/output/upstart/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb
	package_cloud push contribsys/inspeqtor/el/7 packaging/output/systemd/$(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm
	package_cloud push contribsys/inspeqtor/el/6 packaging/output/upstart/$(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm

.PHONY: all clean test build package upload
