#!/bin/sh
# Backport DuckDB PR #27791 without changing the persisted DuckDB 1.4 format.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test "$(go env GOOS)" = linux
case "$(go env GOARCH)" in
  amd64|arm64) ;;
  *) echo 'Unsupported runtime architecture' >&2; exit 1 ;;
esac
version=$(go list -m -f '{{.Version}}' github.com/duckdb/duckdb-go-bindings)
test "$version" = v0.1.21
archive="$(go env GOMODCACHE)/github.com/duckdb/duckdb-go-bindings/linux-$(go env GOARCH)@$version/libparquet_extension.a"
test -f "$archive"
test "$(ar t "$archive" | grep -cx 'decode.cpp.o')" = 1
# Verify the downloaded modules before intentionally backporting the native
# decoder. Keep its cache path: Go plugins must use the host's exact paths.
go mod verify

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
curl --fail --location --silent --show-error --retry 3 \
  https://codeload.github.com/duckdb/duckdb/tar.gz/refs/tags/v1.4.1 \
  --output "$work/source.tar.gz"
echo "91e55efe2c1627c4432d620ee9d2ffcd72f954699e76d7dab523348a7dfbb00a  $work/source.tar.gz" | sha256sum --check -
tar -xzf "$work/source.tar.gz" -C "$work" \
  duckdb-1.4.1/third_party/brotli
source_dir="$work/duckdb-1.4.1"
git -C "$source_dir" apply --check "$script_dir/duckdb-brotli.patch"
git -C "$source_dir" apply "$script_dir/duckdb-brotli.patch"

# Compile against the pinned headers and test the existing archive before
# replacing only its decoder object. Neither C API nor DuckDB storage changes.
c++ -O2 -I"$source_dir/third_party/brotli/include" \
  "$script_dir/duckdb-brotli-check.cpp" "$archive" -o "$work/before"
"$work/before" valid
"$work/before" -9
c++ -O3 -DNDEBUG -fPIC -I"$source_dir/third_party/brotli/include" \
  -c "$source_dir/third_party/brotli/dec/decode.cpp" -o "$work/decode.cpp.o"
chmod u+w "$archive"
ar r "$archive" "$work/decode.cpp.o"
chmod a-w "$archive"
c++ -O2 -I"$source_dir/third_party/brotli/include" \
  "$script_dir/duckdb-brotli-check.cpp" "$archive" -o "$work/after"
"$work/after" valid
"$work/after" valid chunked
"$work/after" -17
"$work/after" -17 chunked
"$work/after" malformed
