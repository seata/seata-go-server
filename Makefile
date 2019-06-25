RELEASE_VERSION    = $(release_version)

ifeq ("$(RELEASE_VERSION)","")
	RELEASE_VERSION		:= "dev"
endif

ROOT_DIR 	    = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
VERSION_PATH    = $(shell echo $(ROOT_DIR) | sed -e "s;${GOPATH}/src/;;g")pkg/util
LD_GIT_COMMIT   = -X '$(VERSION_PATH).GitCommit=`git rev-parse --short HEAD`'
LD_BUILD_TIME   = -X '$(VERSION_PATH).BuildTime=`date +%FT%T%z`'
LD_GO_VERSION   = -X '$(VERSION_PATH).GoVersion=`go version`'
LD_SEATA_VERSION = -X '$(VERSION_PATH).Version=$(RELEASE_VERSION)'
LD_FLAGS        = -ldflags "$(LD_GIT_COMMIT) $(LD_BUILD_TIME) $(LD_GO_VERSION) $(LD_SEATA_VERSION) -w -s"

GOOS 		= linux
CGO_ENABLED = 0
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: darwin
darwin:
	$(eval GOOS := darwin)

.PHONY: release_darwin
release_darwin: darwin release;

.PHONY: proxy_darwin
proxy_darwin: darwin seata-go-proxy;

.PHONY: seata_darwin
seata_darwin: darwin seata-go-server;

.PHONY: dashboard_darwin
dashboard_darwin: darwin seata-go-dashboard;

.PHONY: release
release: dist_dir seata-go-proxy seata-go-seata seata-go-dashboard;

.PHONY: docker
docker: dist_dir ui;
	@echo ========== current docker tag is: $(RELEASE_VERSION) ==========
	sed 's/#TARGET#/seata/g' Dockerfile > Dockerfile.bak
	docker build --build-arg RELEASE=$(RELEASE_VERSION) --build-arg TARGET=seata-go-server -t seata.io/seata-go-server:$(RELEASE_VERSION) -f Dockerfile.bak .
	sed 's/#TARGET#/proxy/g' Dockerfile > Dockerfile.bak
	docker build --build-arg RELEASE=$(RELEASE_VERSION) --build-arg TARGET=seata-go-proxy -t seata.io/seata-go-proxy:$(RELEASE_VERSION) -f Dockerfile.bak .
	sed 's/#TARGET#/dashboard/g' Dockerfile > Dockerfile.bak
	docker build --build-arg RELEASE=$(RELEASE_VERSION) --build-arg TARGET=seata-go-dashboard -t seata.io/seata-go-dashboard:$(RELEASE_VERSION) -f Dockerfile.bak .
	rm -rf *.bak

	docker tag seata.io/seata-go-server:$(RELEASE_VERSION) seata.io/seata-go-server
	docker tag seata.io/seata-go-proxy:$(RELEASE_VERSION) seata.io/seata-go-proxy
	docker tag seata.io/seata-go-dashboard:$(RELEASE_VERSION) seata.io/seata-go-dashboard

.PHONY: ui
ui: dist_dir; $(info ======== compile ui:)
	git clone https://github.com/infinivision/taas-ui.git $(DIST_DIR)ui

.PHONY: seata-go-proxy
seata-go-proxy: dist_dir; $(info ======== compiled seata-go-proxy:)
	env GO111MODULE=on CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod=vendor -a -installsuffix cgo -o $(DIST_DIR)seata-go-proxy $(LD_FLAGS) $(ROOT_DIR)cmd/proxy/*.go

.PHONY: seata-go-server
seata-go-server: dist_dir; $(info ======== compiled seata-go-server:)
	env GO111MODULE=on CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod=vendor -a -installsuffix cgo -o $(DIST_DIR)seata-go-server $(LD_FLAGS) $(ROOT_DIR)cmd/seata/*.go

.PHONY: seata-go-dashboard
seata-go-dashboard: ui; $(info ======== compiled seata-go-dashboard:)
	env GO111MODULE=on CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod=vendor -a -installsuffix cgo -o $(DIST_DIR)seata-go-dashboard $(LD_FLAGS) $(ROOT_DIR)cmd/dashboard/*.go

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)*

.PHONY: clean
clean: ; $(info ======== clean all:)
	rm -rf $(DIST_DIR)*

.PHONY: redis
redis: ; $(info ======== run redis:)
	docker run -d -p 6379:6379 redis

.PHONY: etcd
etcd: ; $(info ======== run etcd:)
	docker run -d -p 2379:2379 xieyanze/etcd3

.PHONY: test
test: redis etcd; $(info ======== run test:)
	env GO111MODULE=on go test -mod=vendor ./...

.PHONY: help
help:
	@echo "build release binary: \n\t\tmake release\n"
	@echo "build docker release with etcd: \n\t\tmake docker\n"
	@echo "clean all binary: \n\t\tmake clean\n"

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
	.DEFAULT_GOAL := release_darwin
else
	.DEFAULT_GOAL := release
endif
