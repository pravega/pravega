<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Pravega Release Notes

Release notes for our latest update, packed with enhancements, fixes, and exciting features.

## Version 0.12
* Spillover from 0.11
* Native client bindings (phase 3: Node.js, Ruby, C#)
* Cross-site replication
* QoS improvements continued
* Performance improvements continued
* Security enhancements continued
* Edge functions
* Shared namespace across Pravega clusters (between edge and core)
* Pravega WebUI

## Version 0.11
* Spillover from 0.10
* Refactor Tier-1
* Native client bindings (phase 2: Golang)
* QoS improvements continued
* Performance improvements continued
* Security enhancements continued

## Version 0.10 (21Q2)
* Spillover from 0.9
* Separate LTS configurations per scope
* Full AWS S3 support for LTS
* Dynamic scaling (scale pods horizontally and vertically according to allocated resources and/or service demand)
* Strengthen video streaming use-cases ([#4087](https://github.com/pravega/pravega/issues/4087))
* Segment Container load balancing ([#1644](https://github.com/pravega/pravega/issues/1644))
* Security-related enhancements
* Quality-of-Service-related features
* Support large events <=1GB ([PDP 43](https://github.com/pravega/pravega/wiki/PDP-43-Large-Events), [#5056](https://github.com/pravega/pravega/issues/5056))
* Improve health checks ([PDP 45](https://github.com/pravega/pravega/wiki/PDP-45-Pravega-Healthcheck), [#5046](https://github.com/pravega/pravega/issues/5046), [#5098](https://github.com/pravega/pravega/issues/5098))
* Idempotent writer ([#1210](https://github.com/pravega/pravega/issues/1210), [#1437](https://github.com/pravega/pravega/issues/1437))
* Performance improvements continued
* Support config-based CDI and Spring client injection

## Version 0.9 (20Q4)
* Improved authorization model
* Simplified-LTS (Long Term Storage) in production ([#4802](https://github.com/pravega/pravega/issues/4802), [#4902](https://github.com/pravega/pravega/issues/4902), [#4903](https://github.com/pravega/pravega/issues/4903), [#4912](https://github.com/pravega/pravega/issues/4912), [#4967](https://github.com/pravega/pravega/issues/4967), [#5067](https://github.com/pravega/pravega/issues/5067))
* Consumption-based retention ([PDP 47](https://github.com/pravega/pravega/wiki/PDP-47:-Pravega-Streams:-Consumption-Based-Retention), [#5108](https://github.com/pravega/pravega/issues/5108), [#5109](https://github.com/pravega/pravega/issues/5109), [#5111](https://github.com/pravega/pravega/issues/5111), [#5112](https://github.com/pravega/pravega/issues/5112), [#5114](https://github.com/pravega/pravega/issues/5114), [#5203](https://github.com/pravega/pravega/issues/5203), [#5306](https://github.com/pravega/pravega/issues/5306))
* Native client bindings, phase 1: Rust Client ([repo](https://github.com/pravega/pravega-client-rust), [design plan](https://github.com/pravega/pravega-client-rust/wiki/Design-plan), [status](https://github.com/pravega/pravega-client-rust/wiki/Supported-APIs)) and Python bindings (Experimental) ([#83](https://github.com/pravega/pravega-client-rust/issues/83), [#113](https://github.com/pravega/pravega-client-rust/issues/113), [#117](https://github.com/pravega/pravega-client-rust/issues/117), [#128](https://github.com/pravega/pravega-client-rust/issues/128), [#164](https://github.com/pravega/pravega-client-rust/issues/164), [#183](https://github.com/pravega/pravega-client-rust/issues/183))
* DR tool v1 ([#4938](https://github.com/pravega/pravega/issues/4938))
* Improve Pravega CLI ([#4803](https://github.com/pravega/pravega/issues/4803), [#5221](https://github.com/pravega/pravega/issues/5221))
* Upgrade server components to Java 11 ([#4884](https://github.com/pravega/pravega/issues/4884), [#5047](https://github.com/pravega/pravega/issues/5047), [#5082](https://github.com/pravega/pravega/issues/5082), [#5232](https://github.com/pravega/pravega/issues/5232), [#5234](https://github.com/pravega/pravega/issues/5234), [#5296](https://github.com/pravega/pravega/issues/5296))
* Performance characterization and improvements: (a) Transaction performance improvements ([#5072](https://github.com/pravega/pravega/issues/5072)), (b) Improvements w/ large # of segments, (c) Schema Registry, (d) Key Value Store, and (e) Simplified-LTS ([#5245](https://github.com/pravega/pravega/issues/5245), [#5260](https://github.com/pravega/pravega/issues/5260))
* K8s operators for Pravega, ZooKeeper & BookKeeper: Tanzu and OpenShift support
* Upgrade base packages for security compliance

