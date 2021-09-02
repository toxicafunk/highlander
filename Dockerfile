FROM ubuntu:18.04
MAINTAINER Toxicafunk <toxicafunk@gmail.com>
ENV TZ=Europe/Madrid
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update &&\
    apt-get upgrade &&\
    apt-get install -y make git zlib1g-dev libssl-dev gperf php-cli cmake clang-6.0 libc++-dev libc++abi-dev &&\
    mkdir /src &&\
    mkdir /src/highlander
RUN curl https://sh.rustup.rs -sSf | sh
WORKDIR /src
RUN git clone https://github.com/tdlib/td.git
WORKDIR /src/td
RUN rm -rf build &&\
    mkdir build
WORKDIR /src/td/build
RUN CXXFLAGS="-stdlib=libc++" CC=/usr/bin/clang-6.0 CXX=/usr/bin/clang++-6.0 cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=../tdlib -DTD_ENABLE_LTO=ON -DCMAKE_AR=/usr/bin/llvm-ar-6.0 -DCMAKE_NM=/usr/bin/llvm-nm-6.0 -DCMAKE_OBJDUMP=/usr/bin/llvm-objdump-6.0 -DCMAKE_RANLIB=/usr/bin/llvm-ranlib-6.0 ..
RUN cmake --build . --target install
RUN apt install -y pkg-config openssl libcrypto++-dev cargo
RUN cp -R /src/td/tdlib/* /usr/local
WORKDIR /src
RUN git clone https://github.com/fewensa/rtdlib.git
WORKDIR /src/highlander
CMD cargo build --release
