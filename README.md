### Prerequisites

The code uses Python, or more specifically, DataStax Python Driver for Apache Cassandra

Please ensure that you have the following packages installed:

* pip
* virtualenv

Additionally, please ensure that you have [downloaded the dataset]

We will also assume that you are using a Linux-based system, or at least have access to a Linux-based terminal

#### Setup

1. Create an env using virtualenv

```
$ # Ensure that you are at the root of this repository
$ virtualenv env
```

2. Install all dependencies in requirement.txt

```
$ source ./env/bin/activate
(env) $ # You should see (env) prepended to the command prompt
(env) $ pip install -r requirements.txt
```

[downloaded the dataset](http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip)
