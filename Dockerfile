FROM ubuntu:22.04

EXPOSE 3333

COPY ./sqlair-bench /sqlair-bench

RUN apt-get update && apt-get install -y --no-install-recommends \
  && apt-get install -y libdqlite-dev 

# On import, go-dqlite sets SQLite to single threaded mode. This causes a seg
# fault if the benchmark is running in SQLite mode. This stops it setting
# single threaded mode. This does apparently incur performance penalties for
# DQLite.
ENV GO_DQLITE_MULTITHREAD=1

ENTRYPOINT ["/sqlair-bench"]
