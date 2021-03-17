#!/bin/bash
git submodule init

find flink-connectors -name "*.md" -\! -wholename "*/.github/*" -print0 | xargs -0 cp --parents -t docs

find pravega-operator -name "*.md" -\! -wholename "*/.github/*" -print0 | xargs -0 cp --parents -t docs
