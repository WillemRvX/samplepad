

makedata_build:
    cd makedata
    docker build -t makedata --build-arg __ENV__=dock_loc .


storage_build:
    cd storage
    docker build -t storage --build-arg __ENV__=dock_loc .


totalcounts_build:
    cd totalcounts
    docker build -t storage \
    --build-arg __ENV__=dock_loc \
    --build-arg __USER__=$(USER) \
    --build-arg __PW__=$(PW)
    .
