#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# ------------------------------------------------------------------------------
# Install Py4J
# ------------------------------------------------------------------------------

# get py4j installed with pip into tox venv
python -m pip install "py4j==0.10.9.7"
#then install jar from tox venv share dir into maven local
py4j_dir="$(python -c "import sys; print(sys.exec_prefix)")/share/py4j"
mvn install:install-file -Dfile="${py4j_dir}/$(ls "${py4j_dir}")" -DgroupId="net.sf.py4j" -DartifactId="py4j" -Dversion="0.10.9.7" -Dpackaging="jar" -DgeneratePom=true
mvn package -Denforcer.skip -Dcheckstyle.skip -Drat.skip=true -DskipTests -pl :flink-python

# ------------------------------------------------------------------------------

if [[ "$@" =~ 'apache-flink-libraries' ]]; then
    pushd apache-flink-libraries
    echo "building apache-flink-libraries..."
    python setup.py sdist
    pushd dist
    echo "installing apache-flink-libraries..."
    python -m pip install apache_flink_libraries-2.0.dev0-py2.py3-none-any.whl
    popd
    popd
fi

if [[ `uname -s` == "Darwin" && `uname -m` == "arm64" ]]; then
  echo "Adding MacOS arm64 GRPC pip install fix"
  export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
  export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
fi

retry_times=3
install_command="python -m pip install $@"
${install_command}
status=$?
while [[ ${status} -ne 0 ]] && [[ ${retry_times} -gt 0 ]]; do
    retry_times=$((retry_times-1))
    # sleep 3 seconds and then reinstall.
    sleep 3
    ${install_command}
    status=$?
done

exit ${status}
