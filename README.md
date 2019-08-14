# xenon-adaptors-cloud

[![Build Status](https://travis-ci.org/xenon-middleware/xenon-adaptors-cloud.svg?branch=master)](https://travis-ci.org/xenon-middleware/xenon-adaptors-cloud)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=xenon-middleware_xenon-adaptors-cloud&metric=alert_status)](https://sonarcloud.io/dashboard?id=xenon-middleware_xenon-adaptors-cloud)
[![codecov](https://codecov.io/gh/xenon-middleware/xenon-adaptors-cloud/branch/master/graph/badge.svg)](https://codecov.io/gh/xenon-middleware/xenon-adaptors-cloud)
[![DOI](https://zenodo.org/badge/136933840.svg)](https://zenodo.org/badge/latestdoi/136933840)

Cloud related adaptors for [xenon library](https://github.com/xenon-middleware/xenon).

Implemented adaptors:

* s3, Xenon filesystem adaptor for [Amazon S3 blob store](https://aws.amazon.com/s3/)
* awsbatch, Xenon scheduler adaptor for [Amazon Batch service](https://aws.amazon.com/batch/)

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

Testing of Xenon adaptor is described in the [Xenon testing document](https://github.com/xenon-middleware/xenon/blob/master/TESTING.md).

Additional integration test requirements:

* [moto_server](https://github.com/spulec/moto), a stand-alone mocked AWS infrastructure server. The `moto_server` executable should be in PATH or location set in MOTO_SERVER environment variable.

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

1. If new adaptor is added,
    1. Add adaptor to `gradle/adaptor.gradle:adaptorDocumentation` and `META-INF/services/nl.esciencecenter.xenon.adaptors.filesystems.FileAdaptor`
    1. Update [adaptor doc](docs/index.html) by running `./gradlew publishSite`
1. Bump version in `README.md`, `build.gradle` and `CITATION.cff`
1. Update CHANGELOG.md
1. Commit and push changes
1. Publish to bintray with `BINTRAY_USER=*** BINTRAY_KEY=**** ./gradlew bintrayUpload`
1. Create GitHub release, for title use the version and for description use first chapter of README as intro and changelog section belonging to the version.
1. In [Zenodo entry](https://doi.org/10.5281/zenodo.3245389), add [Xenon doi](https://doi.org/10.5281/zenodo.597993) as `is referenced by this upload`.
1. Announce release so users of library like xenon-cli can use new version.
