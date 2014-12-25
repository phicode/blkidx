#!/bin/bash

set -e

base=$(dirname "$0")
base=$(readlink -e "$base")
cd "$base"

root_package="github.com/PhiCode/blkidx"
cmd_packages="blkidx"
dependencies="github.com/mattn/go-sqlite3"

go_get_flags="-v"
install_flags=""

if [ $# -ne 0 -a "$1" = "update" ]; then
  # force upgrade packages
  go_get_flags="$go_get_flags -u"
  install_flags="$install_flags -a"
fi

rm -rf coverage
mkdir -p coverage/{json,html,xml}

echo "go-getting coverage utilities"
go get $go_get_flags github.com/axw/gocov/gocov
go get $go_get_flags github.com/AlekSi/gocov-xml
go get $go_get_flags gopkg.in/matm/v1/gocov-html

echo "go-getting dependencies"
for dep in $dependencies; do
  go get $go_get_flags $dep
done

for pkg in $(go list "${root_package}/..."); do
  go test -timeout=5s $pkg
  go test -timeout=5s -coverprofile=c.out $pkg
  go install $pkg
  if [ -e c.out ]; then
    f=$(echo $pkg | tr '/' '_')
    json="coverage/json/${f}.json"
    gocov convert c.out > "$json"
    gocov-xml  < "$json" > "coverage/xml/${f}.xml"
    gocov-html < "$json" > "coverage/html/${f}.html"
    rm -f c.out
  fi
done

for cmd in $cmd_packages; do
	go install $install_flags "${root_package}/cmd/$cmd"
done
