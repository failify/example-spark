Important: This repo contains large files and requires 
[git-lfs](https://git-lfs.github.com/) to be installed 

This repo contains sample [Failify](https://failify.io) test cases for Spark. To make things easier for illustration
purposes, the required files from the build directory of v2.4.3 of spark built on Debian Stretch (using 
``./dev/make-distribution.sh --name custom-spark --pip --tgz -Psparkr -Phadoop-2.7`` command) is included in the repo
so anyone who wants to try the test cases is not required to build spark from scratch.

To run the test cases, first you need to install the following dependencies:
- Java 8
- Docker 1.13+
- Maven 3.x

Also make sure you can run docker commands with the user you are running the tests with. For more information check
[Failify documentation](https://docs.failify.io).

After installing all the dependencies, you can run the test cases using:

```console
$ cd failify
$ mvn test
```