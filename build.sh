#!/usr/bin/env bash
set -Eeo pipefail


if ! /tmp/xq --version >/dev/null 2>&1; then
  curl -fSL# --retry 10 https://github.com/sibprogrammer/xq/releases/download/v1.2.5/xq_1.2.5_linux_amd64.tar.gz | tar -xvz -C /tmp
fi

ver=$(/tmp/xq -x /project/version < pom.xml)
echo "ver=${ver}"
mvn clean package -Dmaven.test.skip=true --file pom.xml