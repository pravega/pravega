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
Instructions to generate Server REST API stubs

## Delete previously generated directory
```
rm -Rf controller/src/main/java/io/pravega/controller/server/rest/generated
```

## Update Controller.yaml
All REST API modifications should be done by updating the swagger/Controller.yaml specification file.
This can be done manually or by using the online editor at http://editor.swagger.io.

## Download Swagger codegen
Download swagger-codegen-cli from maven - https://repo1.maven.org/maven2/io/swagger/swagger-codegen-cli/2.2.3/swagger-codegen-cli-2.2.3.jar

## Generate the API stubs using Swagger Codegen
```
java -jar swagger-codegen-cli.jar generate -i <pravega root>/shared/controller-api/src/main/swagger/Controller.yaml -l jaxrs -c <pravega root>/shared/controller-api/src/main/swagger/server.config.json -o <pravega root>/controller/
```

## Remove extra files created by codegen
All files that get generated outside of the controller/src/main/java/io/pravega/controller/server/rest/generated folder should be deleted and not committed to git.

## Update ApiV1.java
The JAXRS API stubs decorated with swagger annotations are generated in .../server/rest/generated/api/ScopesApi.java class.
Copy these API descriptions into interfaces in .../server/rest/v1/ApiV1.java. Also ensure that the APIs in ApiV1.java are modified to use only jersey async interfaces.

## Update generated/model/RetentionConfig.java
Swagger codegen truncates common enum prefixes. So until https://github.com/swagger-api/swagger-codegen/issues/4261 is fixed we need to perform the following manual step.
In file RetentionConfig.java replace DAYS to LIMITED_DAYS and SIZE_MB to LIMITED_SIZE_MB.

## Generate documentation
### Download Swagger2Markup CLI
https://jcenter.bintray.com/io/github/swagger2markup/swagger2markup-cli/1.3.3/swagger2markup-cli-1.3.3.jar

### Generate and save the markup documentation
```
java -Dswagger2markup.markupLanguage=MARKDOWN -Dswagger2markup.generatedExamplesEnabled=true -jar swagger2markup-cli-1.3.3.jar  convert
 -i <pravega root>/shared/controller-api/src/main/swagger/Controller.yaml -f <pravega root>/documentation/src/docs/rest/restapis
```
