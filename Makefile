NAME=faktory
VERSION=1.4.0

# when fixing packaging bugs but not changing the binary, we increment ITERATION
ITERATION=1
BASENAME=$(NAME)_$(VERSION)-$(ITERATION)

TEST_FLAGS=-parallel 4
ifdef DETECT_RACES
	TEST_FLAGS += -race
endif

# TODO I'd love some help making this a proper Makefile
# with real file dependencies.

.DEFAULT_GOAL := help

all: test

release:
	cp /tmp/faktory-pro_$(VERSION)-$(ITERATION).macos.tbz packaging/output/systemd
	cp /tmp/faktory-ent_$(VERSION)-$(ITERATION).macos.tbz packaging/output/systemd
	@echo Generating release notes
	ruby .github/notes.rb $(VERSION)
	@echo Releasing $(NAME) $(VERSION)-$(ITERATION)
	hub release create v$(VERSION)-$(ITERATION) \
		-a packaging/output/systemd/faktory_$(VERSION)-$(ITERATION)_amd64.deb \
		-a packaging/output/systemd/faktory-$(VERSION)-$(ITERATION).x86_64.rpm \
		-a packaging/output/systemd/faktory-pro_$(VERSION)-$(ITERATION).macos.tbz \
		-a packaging/output/systemd/faktory-ent_$(VERSION)-$(ITERATION).macos.tbz \
	 	-F /tmp/release-notes.md -e -o

prepare: ## install build prereqs
	@go get github.com/benbjohnson/ego/cmd/ego
	@go get github.com/go-bindata/go-bindata/go-bindata
	@echo Now you should be ready to run "make"

tags: clean ## Create tags file for vim, etc
	find . -name "*.go" | grep -v "./vendor" | gotags -L - > tags

test: clean generate ## Execute test suite
	go test $(TEST_FLAGS) \
		github.com/contribsys/faktory/client \
		github.com/contribsys/faktory/cli \
		github.com/contribsys/faktory/manager \
		github.com/contribsys/faktory/server \
		github.com/contribsys/faktory/storage \
		github.com/contribsys/faktory/test \
		github.com/contribsys/faktory/util \
		github.com/contribsys/faktory/webui

dimg: xbuild ## Make a Docker image for the current version
	#eval $(shell docker-machine env default)
	docker build \
		--tag contribsys/faktory:$(VERSION) \
		--tag contribsys/faktory:latest \
		.

drun: ## Run Faktory in a local Docker image, see also "make dimg"
	docker run --rm -it -e "FAKTORY_SKIP_PASSWORD=true" \
		-v faktory-data:/var/lib/faktory \
		-p 127.0.0.1:7419:7419 \
		-p 127.0.0.1:7420:7420 \
		contribsys/faktory:latest /faktory -e production

dmon: ## Monitor Redis within the running Docker image
	docker run --rm -it -t -i \
		-v faktory-data:/var/lib/faktory \
		contribsys/faktory:latest /usr/bin/redis-cli -s /var/lib/faktory/db/redis.sock monitor

#dinsp:
	#docker run --rm -it -e "FAKTORY_PASSWORD=${PASSWORD}" \
		#-p 127.0.0.1:7419:7419 \
		#-p 127.0.0.1:7420:7420 \
		#-v faktory-data:/var/lib/faktory \
		#contribsys/faktory:$(VERSION) /bin/bash

dpush: tag
	docker push contribsys/faktory:$(VERSION)
	docker push contribsys/faktory:latest

generate:
	go generate github.com/contribsys/faktory/webui

cover:
	go test -coverprofile cover.out \
		github.com/contribsys/faktory/cli \
		github.com/contribsys/faktory/client \
		github.com/contribsys/faktory/manager \
		github.com/contribsys/faktory/server \
		github.com/contribsys/faktory/storage \
		github.com/contribsys/faktory/util \
		github.com/contribsys/faktory/webui
	go tool cover -html=cover.out -o coverage.html
	open coverage.html

xbuild: clean generate
	@GOOS=linux GOARCH=amd64 go build -o $(NAME) cmd/faktory/daemon.go
	# brew install upx
	upx -qq ./faktory

build: clean generate
	go build -o $(NAME) cmd/faktory/daemon.go

mon:
	redis-cli -s ~/.faktory/db/redis.sock

# this is a separate target because loadtest doesn't need redis or webui
build_load:
	go build -o loadtest test/load/main.go

load: # not war
	go run test/load/main.go 30000 10

megacheck:
	@megacheck $(shell go list -f '{{ .ImportPath }}'  ./... | grep -ve vendor | paste -sd " " -) || true

# TODO integrate a few useful Golang linters.
lint:
	# brew install golangci/tap/golangci-lint
	golangci-lint run

fmt: ## Format the code
	go fmt ./...

work: ## Run a simple Ruby worker, see also "make run"
	cd test/ruby && bundle exec faktory-worker -v -r ./app.rb -q critical -q default -q bulk

clean: ## Clean the project, set it up for a new build
	@rm -rf tmp
	@rm -f main faktory templates.go
	@rm -rf packaging/output
	@mkdir -p packaging/output/upstart
	@mkdir -p packaging/output/systemd

run: clean generate ## Run Faktory daemon locally
	FAKTORY_PASSWORD=${PASSWORD} go run cmd/faktory/daemon.go -l debug -e development

cssh:
	pushd build/centos && vagrant up && vagrant ssh

ussh:
	pushd build/ubuntu && vagrant up && vagrant ssh

# gem install fpm
# Packaging uses Go's cross compile + fpm so we can build Linux packages on macOS.
package: clean xbuild deb rpm

version_check:
	@grep -q $(VERSION) client/faktory.go || (echo VERSIONS OUT OF SYNC && false)

# these two reload targets are meant to be run within the Vagrant boxes
reload_rpm:
	sudo rpm -e $(NAME)
	sudo yum install -y packaging/output/systemd/$(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm

reload_deb:
	sudo apt-get purge -y $(NAME)
	sudo dpkg -i packaging/output/systemd/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb

rpm: xbuild
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) -p packaging/output/systemd \
		--depends redis \
		--rpm-compression bzip2 \
	 	--rpm-os linux \
	 	--after-install packaging/scripts/postinst.rpm.systemd \
	 	--before-remove packaging/scripts/prerm.rpm.systemd \
		--after-remove packaging/scripts/postrm.rpm.systemd \
		--url https://contribsys.com/faktory \
		--description "Background job server" \
		-m "Contributed Systems LLC <info@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		faktory=/usr/bin/faktory \
		packaging/root/=/

deb: xbuild
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -p packaging/output/systemd \
		--depends redis-server \
		--deb-priority optional --category admin \
		--no-deb-no-default-config-files \
	 	--after-install packaging/scripts/postinst.deb.systemd \
	 	--before-remove packaging/scripts/prerm.deb.systemd \
		--after-remove packaging/scripts/postrm.deb.systemd \
		--url https://contribsys.com/faktory \
		--description "Background job server" \
		-m "Contributed Systems LLC <info@contribsys.com>" \
		--iteration $(ITERATION) --license "GPL 3.0" \
		--vendor "Contributed Systems" -a amd64 \
		faktory=/usr/bin/faktory \
		packaging/root/=/

tag:
	git tag v$(VERSION)-$(ITERATION) && git push --tags || :

upload:	package tag
	package_cloud push contribsys/faktory/ubuntu/xenial packaging/output/systemd/$(NAME)_$(VERSION)-$(ITERATION)_amd64.deb
	package_cloud push contribsys/faktory/el/7 packaging/output/systemd/$(NAME)-$(VERSION)-$(ITERATION).x86_64.rpm

.PHONY: help all clean test build package upload


help:
		@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
