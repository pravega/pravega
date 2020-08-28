import com.github.jk1.license.render.*
import com.github.jk1.license.*
import com.github.jk1.Model.*
import com.github.jk1.license.render.CsvReportRenderer

class PravegaReportRenderer extends CsvReportRenderer {

    List<String> getHeaders() {
        Arrays.asList(
            "Product (Internal code name/project)",
            "URL from where the OSS may be obtained",
            "OSS Title",
            "OSS Version",

            "License Name/Version #",
            "License Category (A, B, C, or D) obtained from the license category index.",
            "Detailed Description of OSS, including its features and functionality",
            "Describe your use of Open Source Software",

            "Do you plan on modifying the code?",
            "Do you plan on distributing the code or any part of it in binary, object, byte, or source code form?",
            "Do you plan on combining code with proprietary code, such as through static linking, dynamic linking, or code level integration?",
            "If combining, please describe how (e.g. static linking, dynamic linking, copy/paste into code) and also describe how the product being combined with the OSS is accessible externally (e.g., shipped on media, downloaded, accessible via the cloud).",

            "Copyright notice found in the OSS package."
        )
    }


    List<String> getFields(ModuleData data) {

        /*
        Product string  `csv:"Product (Internal code name/project)" yaml:"product,omitempty"` //a
        OSSUrl  string  `csv:"URL from where the OSS may be obtained" yaml:"url,omitempty"`   //b
        Title   string  `csv:"OSS Title" yaml:"name,omitempty"`                               //c
        Version Version `csv:"OSS Version" yaml:"version,omitempty"`                          //d

        LicenseName string `csv:"" yaml:"license,omitempty"`                                                                            //e
        Category    string `csv:"License Category (A, B, C, or D) obtained from the license category index." yaml:"category,omitempty"` //f
        Description string `csv:"Detailed Description of OSS, including its features and functionality" yaml:"description,omitempty"`   //g
        DescribeUse string `csv:"Describe your use of Open Source Software" yaml:"use,omitempty"`                                       //h

        WillModify   string `csv:"Do you plan on modifying the code?" yaml:"modify,omitempty"`                                                                                                                                                                                                                        //i
        Distributing string `csv:"Do you plan on distributing the code or any part of it in binary, object, byte, or source code form?" yaml:"distribution,omitempty"`                                                                                                                                                //j
        WillCombine  string `csv:"Do you plan on combining code with proprietary code, such as through static linking, dynamic linking, or code level integration?" yaml:"combining,omitempty"`                                                                                                                       //k
        HowCombine   string `csv:"If combining, please describe how (e.g. static linking, dynamic linking, copy/paste into code) and also describe how the product being combined with the OSS is accessible externally (e.g., shipped on media, downloaded, accessible via the cloud)." yaml:"howCombine,omitempty"` //L

        Copyright string `csv:"Copyright notice found in the OSS package." yaml:"copyright,omitempty"`
        */
        String artifact = String.format("%s:%s", data.group, data.name)

        //Arrays.asList(artifact, getModuleUrl(data), getModuleLicense(data), getModuleLicenseURL(data))
        Arrays.asList(
            "Pravega",
            getModuleUrl(data),
            artifact,
            data.version,

            transformLicense(artifact, getModuleLicense(data)),
            getCategory(artifact, getModuleLicense(data)),
            "Java library",
            "Third-party library",

            "No",
            "Binary",
            "Yes",
            "Dynamic Linking",

            ""// Copyright, going to ignore
        )
    }

    def UNKNOWN = "UNKNOWN"

    String transformLicense(String artifact, String license) {
        def BSD3 = "BSD 3-Clause"
        def BSD2 = "BSD 2-Clause"
        def apache2 = "Apache License, Version 2.0"
        def epl1 = "Eclipse Public License Version 1.0"
        def lgpl21 = "GNU LESSER GENERAL PUBLIC LICENSE, Version 2.1"
        def cddl11 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.1"
        def cddl10 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0"

        if (license == null) {
            def apache2Artifacts = [
                "net.jcip:jcip-annotations", "com.github.fge:jackson-coreutils"
            ]

            if (apache2Artifacts.contains(artifact)) {
                license = apache2
            } else if (artifact.matches("javax.servlet.jsp:jsp-api")) {
                license = "Dual license consisting of the CDDL v1.1 and GPL v2"
            } else {
                return license
            }
        }

        license = license.trim()
        if (artifact.matches("xmlenc:xmlenc")) {
            return BSD3
        }

        if (artifact.matches("org.apache.httpcomponents:httpclient")) {
            return apache2
        }

        if (artifact.matches("org.apache.httpcomponents:httpcore")) {
            return apache2
        }
        if (artifact.matches("com.google.code.findbugs:annotations")) {
            return lgpl21
        }

        if (artifact.matches("org.codehaus.woodstox:stax2-api")) {
            return BSD2
        }

        if (artifact.matches("jline:jline")) {
            return BSD3
        }

        if (artifact.matches("com.thoughtworks.paranamer:paranamer")) {
            return BSD3
        }

        if (artifact.matches("commons-httpclient:commons-httpclient")) {
            return apache2
        }

        if (license.matches("http://www.eclipse.org/legal/epl-v10.html, http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html")) {
            return epl1
        }

        if (license.matches(".*CDDL\\s?\\+\\s?GPL.*")) {
            return cddl11
        }
        if (license.matches(".*CDDL..?1.?1.*GPL.*1.1")) {
            return cddl11
        }

        if (license.matches("Dual license consisting of the CDDL v1.1 and GPL v2")) {
            return cddl11
        }

        if (license.matches("http://repository.jboss.org/licenses/cddl.txt, http://repository.jboss.org/licenses/gpl-2.0-ce.txt")) {
            return cddl10
        }

        if (license.matches("BSD New license")) {
            return BSD3
        }

        if (license.matches("Eclipse Public License - v 1.0")) {
            return epl1 //bundle matches should work :shrug:
        }

        return license
    }

    String getCategory(String artifact, String license) {
        license = transformLicense(artifact, license)

        if (license == null) {
            return null
        }

        def licenses = [:]
        licenses["A"] = [
            "Apache Software License, Version 1.1",
            "BSD 2-Clause",
            "(The )?BSD 3-Clause.*",
            "Creative Commons Legal Code",
            ".*MIT License",
            "PUBLIC DOMAIN",
            "Bouncy Castle Licence",
            "CC0",
            UNKNOWN
        ]
        licenses["B"] = [
            ".*Apache.*2.*",
            "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE \\(CDDL\\) Version 1.0",
            "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE \\(CDDL\\) Version 1.1",
            "Eclipse Public License Version 1.0",
            "Eclipse Public License - v 1.0",
            "GNU GENERAL PUBLIC LICENSE.*",
            "GNU LESSER GENERAL PUBLIC LICENSE, Version 2.1",
            "Mozilla Public License Version 1.1",
            "Mozilla Public License, Version 2.0",
            ".*MPL.?[1-2]\\.[0-1].*"
        ]
        licenses["C"] = [
            "GNU LESSER GENERAL PUBLIC LICENSE, Version 3",
            "LGPLv3.*"
        ]
        licenses["D"] = [
            "Common Public License - v 1.0"
        ]

        for (String category: licenses.keySet()) {
            for (String licensePattern: licenses[category]) {
                if (license.toUpperCase().matches(licensePattern.toUpperCase())) {
                    return category
                }
            }
        }
        return ""
    }
}
