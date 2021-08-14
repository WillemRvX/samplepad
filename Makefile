

gendata_build:
	docker build -t gendata -f ./gendata/Dockerfile \
	--build-arg __ENV__=dock_loc \
	./gendata


gendata_run:
	docker run -d -t gendata


gendata_runit:
	docker run -it gendata bin/bash


storage_build:
	docker build -t storage -f ./storage/Dockerfile \
	--build-arg __ENV__=dock_loc \
	./storage


storage_run:
	docker run -d \
	-e AWS_ACCESS_KEY_ID=$(ACC) \
	-e AWS_DEFAULT_REGION=$(REG) \
	-e AWS_SECRET_ACCESS_KEY=$(SEC) \
	-t storage


storage_runit:
	docker run \
	-e AWS_ACCESS_KEY_ID=$(ACC) \
	-e AWS_DEFAULT_REGION=$(REG) \
	-e AWS_SECRET_ACCESS_KEY=$(SEC) \
	-it storage bin/bash


totalcounts_build:
	docker build -t storage -f ./totalcounts/Dockerfile \
	--build-arg __ENV__=dock_loc \
	./totalcounts


totalcounts_run:
	docker run -d \
	-e USER=$(USER) \
	-e PW=$(PW) \
	-t totalcounts


totalcounts_runit:
	docker run \
	-e USER=$(USER) \
	-e PW=$(PW) \
	-it totalcounts bin/bash


buildall:
	make gendata_build
	make storage_build
	make totalcounts_build


runall:
	make gendata_run
	make storage_run ACC=$(ACC) SEC=$(SEC) REG=$(REG)
	make totalcounts_run USER=$(USER) PW=$(PW)

