# Qumulo REST API -- Sample/ Starter Project Template

## Prerequisites

* Python 2.7
* Node 0.12.4 or greater

## Installing the Prerequisites

### Python

if you're developing on a current version of Mac OSX, you should already have a 2.7 version of python.  you can check which version of python you have by opening a command promopt and typing

  python -V

To install Python 2.7 please visit the [Python Software Foundation
Download Page](https://www.python.org/downloads/)  and select the most
current version (at time of writing it is version 2.7.10)

### Node

On OSX yu can run

```
  brew install node
```

to install the latest version, or visit [Joyent/Node](https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager) and follow instructions for your platform.

## Once You Have Prerequisites Installed
### 1. Install the supporting libraries for python and javascript
Just run the local installer shell script to install the libraries
you'll need for python and/or javascript samples:

```
  ./install_tools
```

You should only need to run this once.

### 2. Install the Qumulo REST API Python Wrapper library
Currently the Qumulo REST API python library is available only by
request -- you'll need an oauth2 token from Qumulo in order to download
it.

Once you have a token from Qumulo, update the line in the requirements.txt file in the
installation directory with <your token> as follows:

    git+https://<your token>:x-oauth-basic@github.com/Qumulo/qumulo_rest_api

and then rerun

```
  pip install -r requirements.txt
```

You can verify that you have the Qumulo REST API installed by running
the following command at a command prompt:
```
  pip list
```
You should see something like the following output:


```

astroid (1.3.6)

logilab-common (1.0.1)

nose (1.3.7)

pip (7.1.0)

pylint (1.4.4)

qumulo-api (1.2.6)

setuptools (17.0)

six (1.9.0)

wheel (0.24.0)

```



### 3. Kick the Tires
To get started the project includes a sample command `get_stats.py` (and an associated test) that will use the Qumulo REST API to get filesystem stats for a specified cluster; you can run it like this (changing the host, port and user acct/pwd parameters to match your local environment):

```
/get_stats.py --host dev --user clusteruser --pass pwd --port 20095
```

your output from running this command should look something like this:

```
{
    "block_size_bytes": 4096,
    "free_size_bytes": "11254198272",
    "raw_size_bytes": "28521267200",
    "total_size_bytes": "11254276096"
}
```

### 4. Check your code quality
We support [Pylint](http://www.pylint.org/) and [JsHint](http://jshint.com/) / [EsLint](http://eslint.org/) to ensure that sample code meets coding standards such as [PEP8](https://www.python.org/dev/peps/pep-0008/) and Qumulo's own coding standards for Python and Javascript.  To check your code against the checking rules, just run

```

  ./runlint

```


from the directory where you downloaded this sample project.

You should `./runlint` each time you update your source code.


### 5. Create and run tests to verify that your sample works
We support Python unit tests via [Nose](http://pythontesting.net/framework/nose/nose-introduction/) and Javascript tests using [Jasmine](http://jasmine.github.io/2.3/introduction.html).  You can run all python and javascript tests in the current directory (one python test is included by default) by running the following shell script:

```

  ./runtests

```

Ideally you should create tests that exercise your sample code, so you can easily tell if your sample works when we drop new versions of the Qumulo REST API framework or when you've made changes to your sample.  
