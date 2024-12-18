plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-client:3.4.1")
    implementation("org.apache.hadoop:hadoop-common:3.4.1")
    implementation("org.apache.hadoop:hadoop-hdfs:3.4.1")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

application {
    // Define the main class for the application.
    mainClass = "slowlime.Main"
}
