# xenon-adaptors-cloud

[![Build Status](https://travis-ci.org/xenon-middleware/xenon-adaptors-cloud.svg?branch=master)](https://travis-ci.org/xenon-middleware/xenon-adaptors-cloud)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=xenon-middleware_xenon-adaptors-cloud&metric=alert_status)](https://sonarcloud.io/dashboard?id=xenon-middleware_xenon-adaptors-cloud)
[![codecov](https://codecov.io/gh/xenon-middleware/xenon-adaptors-cloud/branch/master/graph/badge.svg)](https://codecov.io/gh/xenon-middleware/xenon-adaptors-cloud)
[![DOI](https://zenodo.org/badge/136933840.svg)](https://zenodo.org/badge/latestdoi/136933840)

Cloud related adaptors for [xenon library](https://github.com/xenon-middleware/xenon).

Implemented adaptors:
* s3, Xenon filesystem adaptor for [Amazon S3 blob store](https://aws.amazon.com/s3/)
* azureblob, Xenon filesystem adaptor for [Azure blob store](https://azure.microsoft.com/en-us/services/storage/blobs/)

See [adaptor documentation](https://xenon-middleware.github.io/xenon-adaptors-cloud/) for what each adaptor offers and requires. 
 
## Usage

The library can be added as a Gradle dependency to your own project with
```groovy
repositories {
    // ... others
    jcenter()
}
dependencies {
    // ... others
    implementation group: 'nl.esciencecenter.xenon.adaptors', name: 'xenon-adaptors-cloud', version: '3.0.2'
}
```

## Testing

See https://github.com/xenon-middleware/xenon/blob/master/TESTING.md

To run live test on AWS S3, first create a bucket containing the test fixtures listed in [create_symlinks script](https://github.com/xenon-middleware/xenon/blob/master/src/liveTest/resources/scripts/create_symlinks)
and then run live test command:

```sh
./gradlew liveTest -Dxenon.filesystem=s3 \
 -Dxenon.filesystem.location=https://s3.<region>.amazonaws.com/<bucket> \
 -Dxenon.username=<access key> -Dxenon.password=<secret key> \
 -Dxenon.filesystem.basedir=/ -Dxenon.filesystem.expected.workdir=/
```

To run the live tests on Microsoft Azure, first create a container containing the test fixtures mentioned above and run the following test command: 

```sh
./gradlew liveTest -Dxenon.filesystem=azure \
 -Dxenon.filesystem.location=https://<user>.blob.core.windows.net/<container> \
 -Dxenon.username=<access key> -Dxenon.password=<secret key> \
 -Dxenon.filesystem.basedir=/ -Dxenon.filesystem.expected.workdir=/
```

## New release

Chapter is for xenon developers.

The major version should be the same as the used xenon library.

1. If new adaptor is added, also add it to `gradle/adaptor.gradle:adaptorDocumentation` and `./src/main/resources/META-INF/services/nl.esciencecenter.xenon.adaptors.filesystems.FileAdaptor`
1. Bump version in `README.md`, `build.gradle` and `CITATION.cff`, update CHANGELOG.md and commit/push
1. Publish to bintray with `BINTRAY_USER=*** BINTRAY_KEY=**** ./gradlew bintrayUpload`
1. Create GitHub release
1. In [Zenodo entry](https://doi.org/10.5281/zenodo.3245389), add [Xenon doi](https://doi.org/10.5281/zenodo.597993) as `is referenced by this upload`.
1. Announce release so users of library like xenon-cli can use new version.
