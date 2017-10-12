### Prerequisites

The code uses Python (Python 2.7.5), or more specifically, DataStax Python Driver for Apache Cassandra

Please ensure that you have the `virtualenv` and `pip` packages installed.

Please also ensure that you have [downloaded the dataset](http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip)

Please take note of the file path for your `cqlsh` - you will need to confirm the file path for `cqlsh`

Additionally, please ensure that all the nodes have `cassandra` running.

We will also assume that you are using a Linux-based system, or at least have access to a Linux-based terminal

In summary, ensure the following:

* pip (Python package)
* virtualenv (Python package)
* File path for `cqlsh`
* `cassandra` running on all nodes
* [Dataset](http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip)

### Setting up virtualenv

The following code will circumvent the restriction where users are not able to use `pip` to install the package in `/usr/lib/python2.7/site-packages`

1. Download the `virtualenv` source code, and run the script locally. At the end, you should get the full path of the file, `virtualenv.py`

```
$ curl -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
$ tar xvfz virtualenv-1.9.tar.gz
$ cd virtualenv-1.9
$ realpath virtualenv.py
$ # The full path of virtualenv.py will be obtained, take note of the path
$ # A possible output: /home/stuproj/cs4224f/virtualenv-1.9/virtualenv.py (results will vary)
```

### Setup

0. Download and unzip the project repository

```
$ # Assume that the project directory is placed in the home directory
$ cd ~
$ unzip <Project File>
```

1. Create an environment directory, `env`, using virtualenv. Replace the variable <virtualenv> with the full path of the file, `virtualenv.py`

```
$ # Ensure that you are at the root of the project repository
$ cd <Project File>
$ <virtualenv> env
$ # The following is an example
$ # /home/stuproj/cs4224f/virtualenv-1.9/virtualenv.py env
```

2. Install all dependencies in requirement.txt

```
$ source ./env/bin/activate
(env) $ # You should see (env) prepended to the command prompt
(env) $ pip install -r requirements.txt
```

3. Download the dataset (.csv file) if you have not, and move them to /data folder

```
(env) $ wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip
(env) $ unzip 4224-project-files.zip
(env) $ mv ./4224-project-files/data-files/*.csv ./data
```

4. Run `import.py` to import the data into cassandra

```
(env) $ python import.py
```

5. You will be prompted to confirm the correct `cqlsh` path

<<Insert Image>>

6. By the completion of the script, the data would have been imported into the cassandra database.

### Running Cassandra with DataStax Python Driver


### Exit

* To exit Cassandra

```
$ ps auwx | grep cassandra
$ # Get the pid; remember the pid - we will be using it later
$ kill pid
```

* To exit the env

```
(env) $ deactivate
```
