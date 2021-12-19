FROM rust:1.56

RUN apt install git --fix-missing

COPY . /root/FedTruthFinderImplOffline/
WORKDIR /root/FedTruthFinderImplOffline
RUN cargo update
RUN rustup component add rustfmt
RUN cargo install --path . --bins
RUN cargo clean
