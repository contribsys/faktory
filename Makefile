NAME=faktory
VERSION=0.5.0

# when fixing packaging bugs but not changing the binary, we increment ITERATION
ITERATION=1
BASENAME=$(NAME)_$(VERSION)-$(ITERATION)


# contains various secret or machine-specific variables.
# DEB_PRODUCTION: hostname of a debian-based upstart machine (e.g. Ubuntu {12,14}.04 LTS)
# RPM_PRODUCTION: hostname of a redhat-based systemd machine (e.g. CentOS 7)
#include $(HOME)/.local.sh

# TODO I'd love some help making this a proper Makefile
# with real file dependencies.

all: test

prepare:
	go get github.com/benbjohnson/ego/cmd/ego
	go get github.com/stretchr/testify/...
	go get github.com/jteeuwen/go-bindata/...
	go get github.com/sirupsen/logrus
	# necessary to compile the gorocksdb bindings
	#ROCKSDB_HOME=/home/ubuntu/rocksdb
	#CGO_CFLAGS="-I${ROCKSDB_HOME}/include"
	#CGO_LDFLAGS="-L${ROCKSDB_HOME}"
	# make won't export these variables so they must be set up outside this Makefile
	# Use "go get -x" to debug compilation problems.
	go get github.com/mperham/gorocksdb
	#gem install -N fpm
	@echo Now you should be ready to run "make"

test: clean generate
	go test -parallel 4 \
		github.com/mperham/faktory \
		github.com/mperham/faktory/server \
		github.com/mperham/faktory/storage \
		github.com/mperham/faktory/test \
		github.com/mperham/faktory/util \
		github.com/mperham/faktory/webui

generate:
	go generate ./...

cover:
	go test -cover -coverprofile cover.out github.com/mperham/faktory/webui
	go tool cover -html=cover.out -o coverage.html
	/Applications/Firefox.app/Contents/MacOS/firefox coverage.html

# we can't cross-compile when using cgo <cry>
#	@GOOS=linux GOARCH=amd64
build: clean generate
	go build -o faktory cmd/daemon.go

# goimports produces slightly different formatted code from go fmt
# TODO integrate a few useful Golang linters.
fmt:
	go fmt ./...

swork:
	cd test/ruby && FAKTORY_PROVIDER=FURL \
		FURL=tcp://localhost.contribsys.com:7419 \
		bundle exec faktory-worker -v -r ./app.rb -q critical -q default -q bulk

work:
	cd test/ruby && bundle exec faktory-worker -v -r ./app.rb -q critical -q default -q bulk

clean:
	@rm -f webui/*.ego.go
	@rm -rf tmp
	@rm -f main faktory templates.go
	@rm -rf packaging/output
	@mkdir -p packaging/output/upstart
	@mkdir -p packaging/output/systemd

repl: clean generate
	go run cmd/repl.go -l debug -d .

run: clean generate
	go run cmd/daemon.go -l debug -d .

srun: clean generate
	go run cmd/daemon.go -b 127.0.0.1:7419 -l debug -d .

cssh:
	pushd build/centos && vagrant up && vagrant ssh

ussh:
	pushd build/ubuntu && vagrant up && vagrant ssh

# gem install fpm
# https://github.com/jordansissel/fpm/issues/576
# brew install gnu-tar
# ln -s /usr/local/bin/gtar /usr/local/bin/gnutar
package: version_check build_deb_systemd build_rpm_systemd

version_check:
	@grep -q $(VERSION) faktory.go || (echo VERSIONS OUT OF SYNC && false)

purge_deb:
	ssh -t $(DEB_PRODUCTION) 'sudo apt-get purge -y $(NAME) && sudo rm -f /etc/faktory' || true

purge_rpm:
	ssh -t $(RPM_PRODUCTION) 'sudo rpm -e $(NAME) && sudo rm -f /etc/faktory' || true

deploy_deb: clean build_deb purge_deb
	scp packaging/output/upstart/*.deb $(DEB_PRODUCTION):~
	ssh $(DEB_PRODUCTION) 'sudo rm -f /etc/faktory && sudo dpkg -i $(NAME)_$(VERSION)-$(ITERATION)_amd64.deb && sudo ./fix && sudo restart faktory || true'

deploy_rpm: clean build_rpm purge_rpm
	scp packaging/output/systemd/*.rpm $(RPM_PRODUCTION):~
	ssh -t $(RPM_PRODUCTION) 'sudo rm -f /etc/faktory && sudo yum install -q -y $(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm && sudo ./fix && sudo systemctl restart faktory'

update_deb: clean build_deb
	scp packaging/output/upstart/*.deb $(DEB_PRODUCTION):~
	ssh $(DEB_PRODUCTION) 'sudo dpkg -i $(NAME)_$(VERSION)-$(ITERATION)_amd64.deb'

update_rpm: clean build_rpm
	scp packaging/output/systemd/*.rpm $(RPM_PRODUCTION):~
	ssh -t $(RPM_PRODUCTION) 'sudo yum install -q -y $(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm'

deploy: deploy_deb deploy_rpm
purge: purge_deb purge_rpm

build_rpm_upstart:
	# gem install fpm
	# brew install rpm
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p packaging/output/upstart \
		--rpm-compression bzip2 --rpm-os linux \
	 	--after-install packaging/scripts/postinst.rpm.upstart \
	 	--before-remove packaging/scripts/prerm.rpm.upstart \
		--after-remove packaging/scripts/postrm.rpm.upstart \
		--url http://contribsys.com/faktory \
		--description "Background job server" \
		-m "Contributed Systems LLC <info@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		faktory=/usr/bin/faktory \
		packaging/root/=/

build_rpm_systemd:
	# gem install fpm
	# brew install rpm
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p packaging/output/systemd \
		--rpm-compression bzip2 --rpm-os linux \
	 	--after-install packaging/scripts/postinst.rpm.systemd \
	 	--before-remove packaging/scripts/prerm.rpm.systemd \
		--after-remove packaging/scripts/postrm.rpm.systemd \
		--url http://contribsys.com/faktory \
		--description "Background job server" \
		-m "Contributed Systems LLC <info@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		faktory=/usr/bin/faktory \
		packaging/root/=/

build_deb_upstart:
	# gem install fpm
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p packaging/output/upstart \
		--deb-priority optional --category admin \
		--deb-compression bzip2 \
		--no-deb-no-default-config-files \
	 	--after-install packaging/scripts/postinst.deb.upstart \
	 	--before-remove packaging/scripts/prerm.deb.upstart \
		--after-remove packaging/scripts/postrm.deb.upstart \
		--url http://contribsys.com/faktory \
		--description "Background job server" \
		-m "Contributed Systems LLC <info@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		faktory=/usr/bin/faktory \
		packaging/root/=/

build_deb_systemd:
	# gem install fpm
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p packaging/output/systemd \
		--deb-priority optional --category admin \
		--deb-compression bzip2 \
		--no-deb-no-default-config-files \
	 	--after-install packaging/scripts/postinst.deb.systemd \
	 	--before-remove packaging/scripts/prerm.deb.systemd \
		--after-remove packaging/scripts/postrm.deb.systemd \
		--url http://contribsys.com/faktory \
		--description "Background job server" \
		-m "Contributed Systems LLC <info@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		faktory=/usr/bin/faktory \
		packaging/root/=/

tag:
	git tag v$(VERSION)-$(ITERATION) && git push --tags || :

upload:	package tag
	# gem install -N package_cloud
	package_cloud push contribsys/faktory/ubuntu/xenial packaging/output/systemd/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb
	#package_cloud push contribsys/faktory/ubuntu/trusty packaging/output/upstart/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb
	package_cloud push contribsys/faktory/el/7 packaging/output/systemd/$(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm
	#package_cloud push contribsys/faktory/el/6 packaging/output/upstart/$(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm

.PHONY: all clean test build package upload
