RELEASE_VERSION    = $(release_version)

ifeq ("$(RELEASE_VERSION)","")
	RELEASE_VERSION		:= "dev"
endif

ROOT_DIR 	    = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
VERSION_PATH    = $(shell echo $(ROOT_DIR) | sed -e "s;${GOPATH}/src/;;g")pkg/util
LD_GIT_COMMIT   = -X '$(VERSION_PATH).GitCommit=`git rev-parse --short HEAD`'
LD_BUILD_TIME   = -X '$(VERSION_PATH).BuildTime=`date +%FT%T%z`'
LD_GO_VERSION   = -X '$(VERSION_PATH).GoVersion=`go version`'
LD_TAAS_VERSION = -X '$(VERSION_PATH).Version=$(RELEASE_VERSION)'
LD_FLAGS        = -ldflags "$(LD_GIT_COMMIT) $(LD_BUILD_TIME) $(LD_GO_VERSION) $(LD_TAAS_VERSION) -w -s"

GOOS 		= linux
CGO_ENABLED = 0
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: darwin
darwin:
	$(eval GOOS := darwin)

.PHONY: release_darwin
release_darwin: darwin release;

.PHONY: proxy_darwin
proxy_darwin: darwin proxy;

.PHONY: seata_darwin
seata_darwin: darwin seata;

.PHONY: dashboard_darwin
dashboard_darwin: darwin dashboard;

.PHONY: release
release: dist_dir proxy seata dashboard;

.PHONY: docker
docker: dist_dir ui;
	@echo ========== current docker tag is: $(RELEASE_VERSION) ==========
	sed 's/#TARGET#/seata/g' Dockerfile > Dockerfile.bak
	docker build --build-arg RELEASE=$(RELEASE_VERSION) --build-arg TARGET=seata -t infinivision/seata:$(RELEASE_VERSION) -f Dockerfile.bak .
	sed 's/#TARGET#/proxy/g' Dockerfile > Dockerfile.bak
	docker build --build-arg RELEASE=$(RELEASE_VERSION) --build-arg TARGET=proxy -t infinivision/seata-proxy:$(RELEASE_VERSION) -f Dockerfile.bak .
	sed 's/#TARGET#/dashboard/g' Dockerfile > Dockerfile.bak
	docker build --build-arg RELEASE=$(RELEASE_VERSION) --build-arg TARGET=dashboard -t infinivision/seata-dashboard:$(RELEASE_VERSION) -f Dockerfile.bak .
	rm -rf *.bak

	docker tag infinivision/seata:$(RELEASE_VERSION) infinivision/seata
	docker tag infinivision/seata-proxy:$(RELEASE_VERSION) infinivision/seata-proxy
	docker tag infinivision/seata-dashboard:$(RELEASE_VERSION) infinivision/seata-dashboard

.PHONY: ui
ui: dist_dir; $(info ======== compile ui:)
	git clone https://github.com/infinivision/taas-ui.git $(DIST_DIR)ui

.PHONY: proxy
proxy: dist_dir; $(info ======== compiled proxy:)
	env GO111MODULE=on CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod=vendor -a -installsuffix cgo -o $(DIST_DIR)proxy $(LD_FLAGS) $(ROOT_DIR)cmd/proxy/*.go

.PHONY: seata
seata: dist_dir; $(info ======== compiled seata:)
	env GO111MODULE=on CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod=vendor -a -installsuffix cgo -o $(DIST_DIR)seata $(LD_FLAGS) $(ROOT_DIR)cmd/seata/*.go

.PHONY: dashboard
dashboard: dist_dir; $(info ======== compiled dashboard:)
	env GO111MODULE=on CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -mod=vendor -a -installsuffix cgo -o $(DIST_DIR)dashboard $(LD_FLAGS) $(ROOT_DIR)cmd/dashboard/*.go

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)*

.PHONY: clean
clean: ; $(info ======== clean all:)
	rm -rf $(DIST_DIR)*

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
