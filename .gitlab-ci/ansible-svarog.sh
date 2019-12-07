#!/bin/bash
#*******************************************************************************
# Copyright (c) 2013, 2017 Perun Technologii DOOEL Skopje.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License
# Version 2.0 or the Svarog License Agreement (the "License");
# You may not use this file except in compliance with the License. 
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See LICENSE file in the project root for the specific language governing 
# permissions and limitations under the License.
#
#*******************************************************************************
#set -x


ansible -m ping gitlab-ci-runner -i hosts/gitlabci.yml


if [[ -n $2 ]]; then
  ansible-playbook -i hosts/gitlabci.yml site-svarog.yaml -e "env_name=$1" -e "base_dir=$2" -e "db=$3" -e "CI_PIPELINE_ID=$4"
else
  echo $2
fi
