# mac 交叉编译

## 下载交叉编译工具
```
mkdir ~/Downloads/macos-cross-toolchains
cd ~/Downloads/macos-cross-toolchains

wget https://github.com/messense/homebrew-macos-cross-toolchains/releases/download/v13.2.0/aarch64-unknown-linux-musl-aarch64-darwin.tar.gz
wget https://github.com/messense/homebrew-macos-cross-toolchains/releases/download/v13.2.0/x86_64-unknown-linux-musl-x86_64-darwin.tar.gz

tar -xvzf aarch64-unknown-linux-musl-aarch64-darwin.tar.gz
tar -xvzf x86_64-unknown-linux-musl-x86_64-darwin.tar.gz
```

## 添加 rust target
```
rustup target add aarch64-unknown-linux-musl
rustup target add x86_64-unknown-linux-musl
```

## 编译
```
# build x86_64-unknown-linux-musl
export "PATH=$HOME/Downloads/macos-cross-toolchains/x86_64-unknown-linux-musl/bin:$PATH"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc
export CC=x86_64-linux-musl-gcc
sudo cargo build --release --target=x86_64-unknown-linux-musl

# build aarch64-unknown-linux-musl
export "PATH=$HOME/Downloads/macos-cross-toolchains/aarch64-unknown-linux-musl/bin:$PATH"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc
export CC=aarch64-linux-musl-gcc
sudo cargo build --release --target=aarch64-unknown-linux-musl
```

## 生成并推送镜像
```
# copy targets to tmp dir
rm -rf tmp
mkdir -p tmp/amd64-linux
mkdir -p tmp/arm64-linux
cp target/x86_64-unknown-linux-musl/release/dt-main tmp/amd64-linux/
cp target/aarch64-unknown-linux-musl/release/dt-main tmp/arm64-linux/

# build and push images
sudo docker buildx build \
-f Dockerfile.cross \
--platform linux/amd64,linux/arm64 \
--tag apecloud/ape-dts:0.1.18-test.15 \
--push \
.
```
