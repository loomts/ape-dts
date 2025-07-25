# Build Images

# Cross build with github action

You can get amd64 & arm64 images which uses gnu by running [github workflow](/.github/workflows/build_push_images.yml)

# Local build

- on arm64 machine

```
docker buildx build \
--platform linux/arm64 --tag ape-dts:0.1.0-test-arm64 \
--build-arg MODULE_NAME=dt-main --load .
```

- on amd64 machine

```
docker buildx build \
--platform linux/amd64 --tag ape-dts:0.1.0-test-amd64 \
--build-arg MODULE_NAME=dt-main --load .
```

# Cross build on Mac

On Mac, you can use musl to produce a statically linked executable, then copy it into an alpine image.

But note that musl may cause Rust code very slow, refer to: [blog](https://andygrove.io/2020/05/why-musl-extremely-slow/), [discussion](https://www.reddit.com/r/rust/comments/gdycv8/why_does_musl_make_my_code_so_slow/), it is **NOT** recommended for production.

## Download cross build tools

```
mkdir ~/Downloads/macos-cross-toolchains
cd ~/Downloads/macos-cross-toolchains

wget https://github.com/messense/homebrew-macos-cross-toolchains/releases/download/v13.2.0/aarch64-unknown-linux-musl-aarch64-darwin.tar.gz
wget https://github.com/messense/homebrew-macos-cross-toolchains/releases/download/v13.2.0/x86_64-unknown-linux-musl-x86_64-darwin.tar.gz

tar -xvzf aarch64-unknown-linux-musl-aarch64-darwin.tar.gz
tar -xvzf x86_64-unknown-linux-musl-x86_64-darwin.tar.gz
```

## Add rust target

```
rustup target add aarch64-unknown-linux-musl
rustup target add x86_64-unknown-linux-musl
```

## Build

```
# build x86_64-unknown-linux-musl
export "PATH=$HOME/Downloads/macos-cross-toolchains/x86_64-unknown-linux-musl/bin:$PATH"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc
export CC=x86_64-linux-musl-gcc
sudo cargo build --release --features metrics --target=x86_64-unknown-linux-musl

# build aarch64-unknown-linux-musl
export "PATH=$HOME/Downloads/macos-cross-toolchains/aarch64-unknown-linux-musl/bin:$PATH"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc
export CC=aarch64-linux-musl-gcc
sudo cargo build --release --features metrics --target=aarch64-unknown-linux-musl
```

## Build images and push

```
# copy targets to tmp dir
rm -rf tmp
mkdir -p tmp/amd64-linux
mkdir -p tmp/arm64-linux
cp target/x86_64-unknown-linux-musl/release/dt-main tmp/amd64-linux/
cp target/aarch64-unknown-linux-musl/release/dt-main tmp/arm64-linux/

# build and push images
sudo docker buildx build \
-f Dockerfile.mac.cross \
--platform linux/amd64,linux/arm64 \
--tag apecloud/ape-dts:0.1.18-test.15 \
--push \
.
```
