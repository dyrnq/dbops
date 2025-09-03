#!/usr/bin/env bash
set -Eeo pipefail


if ! /tmp/xq --version ; then
  curl -fSL# --retry 10 https://github.com/sibprogrammer/xq/releases/download/v1.2.5/xq_1.2.5_linux_amd64.tar.gz | tar -xvz -C /tmp
fi

ver=$(cat pom.xml | /tmp/xq -x /project/version)
echo "ver=${ver}"
mvn clean package -Dmaven.test.skip=true --file pom.xml