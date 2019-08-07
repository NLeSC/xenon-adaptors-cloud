# xenon-adaptors-cloud

[![Build Status](https://travis-ci.org/xenon-middleware/xenon-adaptors-cloud.svg?branch=master)](https://travis-ci.org/xenon-middleware/xenon-adaptors-cloud)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=xenon-middleware_xenon-adaptors-cloud&metric=alert_status)](https://sonarcloud.io/dashboard?id=xenon-middleware_xenon-adaptors-cloud)
[![codecov](https://codecov.io/gh/xenon-middleware/xenon-adaptors-cloud/branch/master/graph/badge.svg)](https://codecov.io/gh/xenon-middleware/xenon-adaptors-cloud)
[![DOI](https://zenodo.org/badge/136933840.svg)](https://zenodo.org/badge/latestdoi/136933840)

Cloud related adaptors for [xenon library](https://github.com/xenon-middleware/xenon).

Implemented adaptors:
* s3, Xenon filesystem adaptor for [Amazon S3 blob store](https://aws.amazon.com/s3/)
 
## Usage

The library can be added as a Gradle dependency to your own project with
```groovy
repositories {
    // ... others
    jcenter()
}
dependencies {
    // ... others
    implementation group: 'nl.esciencecenter.xenon.adaptors', name: 'xenon-adaptors-cloud', version: '3.0.1'
}
```

## Testing

See https://github.com/xenon-middleware/xenon/blob/master/TESTING.md

To run live test on AWS S3, first create the test fixtures listed in [create_symlinks script](https://github.com/xenon-middleware/xenon/blob/master/src/liveTest/resources/scripts/create_symlinks)
and then run live test command:

```sh
./gradlew liveTest -Dxenon.filesystem=s3 \
 -Dxenon.filesystem.location=https://s3.<region>.amazonaws.com/<bucket> \
 -Dxenon.username=<access key> -Dxenon.password=<secret key> \
 -Dxenon.basedir=/ -Dxenon.filesystem.expected.workdir=/
```

## New release

Chapter is for xenon developers.

The major version should be the same as the used xenon library.

1. Bump version in `README.md`, `build.gradle` and `CITATION.cff`, update CHANGELOG.md and commit/push
1. Publish to bintray with `BINTRAY_USER=*** BINTRAY_KEY=**** ./gradlew bintrayUpload`
1. Create GitHub release
1. Announce release so users of library like xenon-cli can use new version.
