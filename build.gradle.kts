plugins {
    java
    application
}

application {
    mainClassName = "App"
}

dependencies {
    compile("com.google.cloud:google-cloud-pubsub:1.45.0")
}

repositories {
    mavenLocal()
    jcenter()
}
